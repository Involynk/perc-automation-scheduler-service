import { Test, TestingModule } from '@nestjs/testing';
import { StartupRecoveryService } from './startup-recovery.service';
import { PrismaService } from '../../infrastructure/persistence/prisma.service';
import { TimerQueueService } from '../../infrastructure/bullmq/timer-queue.service';
import { TimerStatus } from '../../domain/enums/timer-status.enum';

describe('StartupRecoveryService (Reconciliation Tests)', () => {
  let service: StartupRecoveryService;
  let mockPrisma: any;
  let mockQueueService: Partial<TimerQueueService>;

  beforeEach(async () => {
    mockPrisma = {
      timer: {
        findMany: jest.fn(),
      },
    };

    mockQueueService = {
      getJob: jest.fn(),
      addDelayedTimer: jest.fn().mockResolvedValue(undefined),
      removeTimerJob: jest.fn().mockResolvedValue(true),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        StartupRecoveryService,
        { provide: PrismaService, useValue: mockPrisma },
        { provide: TimerQueueService, useValue: mockQueueService },
      ],
    }).compile();

    service = module.get<StartupRecoveryService>(StartupRecoveryService);
  });

  it('Scenario 1: should remove stale BullMQ job if timer is CANCELLED', async () => {
    mockPrisma.timer.findMany.mockResolvedValue([
      {
        id: 't-1',
        timerKey: 'lead:L-1:fup',
        status: TimerStatus.CANCELLED,
        targetExecutionTime: new Date(Date.now() + 5000),
      },
    ]);
    (mockQueueService.getJob as jest.Mock).mockResolvedValue({ id: 'lead:L-1:fup' });

    const result = await service.reconcileTimers();

    expect(mockQueueService.removeTimerJob).toHaveBeenCalledWith('lead:L-1:fup');
    expect(result.reconciledCount).toBe(1);
  });

  it('Scenario 2: should remove stale BullMQ job if timer is EXECUTED', async () => {
    mockPrisma.timer.findMany.mockResolvedValue([
      {
        id: 't-2',
        timerKey: 'lead:L-2:fup',
        status: TimerStatus.EXECUTED,
        targetExecutionTime: new Date(Date.now() - 5000),
      },
    ]);
    (mockQueueService.getJob as jest.Mock).mockResolvedValue({ id: 'lead:L-2:fup' });

    const result = await service.reconcileTimers();

    expect(mockQueueService.removeTimerJob).toHaveBeenCalledWith('lead:L-2:fup');
    expect(result.reconciledCount).toBe(1);
  });

  it('Scenario 3: should re-enqueue missing BullMQ job if timer is PENDING and in future', async () => {
    const futureTime = new Date(Date.now() + 100000);
    mockPrisma.timer.findMany.mockResolvedValue([
      {
        id: 't-3',
        timerKey: 'lead:L-3:fup',
        status: TimerStatus.PENDING,
        targetExecutionTime: futureTime,
      },
    ]);
    (mockQueueService.getJob as jest.Mock).mockResolvedValue(null);

    const result = await service.reconcileTimers();

    expect(mockQueueService.addDelayedTimer).toHaveBeenCalledWith('t-3', 'lead:L-3:fup', expect.any(Number));
    expect(result.reconciledCount).toBe(1);
  });

  it('Scenario 4: should enqueue with delay=0 if timer is PENDING and overdue (past due)', async () => {
    const pastTime = new Date(Date.now() - 5000);
    mockPrisma.timer.findMany.mockResolvedValue([
      {
        id: 't-4',
        timerKey: 'lead:L-4:fup',
        status: TimerStatus.PENDING,
        targetExecutionTime: pastTime,
      },
    ]);
    (mockQueueService.getJob as jest.Mock).mockResolvedValue(null);

    const result = await service.reconcileTimers();

    expect(mockQueueService.addDelayedTimer).toHaveBeenCalledWith('t-4', 'lead:L-4:fup', 0);
    expect(result.reconciledCount).toBe(1);
  });

  it('Scenario 5: should leave job untouched if PENDING and BullMQ job already exists', async () => {
    const futureTime = new Date(Date.now() + 100000);
    mockPrisma.timer.findMany.mockResolvedValue([
      {
        id: 't-5',
        timerKey: 'lead:L-5:fup',
        status: TimerStatus.PENDING,
        targetExecutionTime: futureTime,
      },
    ]);
    (mockQueueService.getJob as jest.Mock).mockResolvedValue({ id: 'lead:L-5:fup' });

    const result = await service.reconcileTimers();

    expect(mockQueueService.addDelayedTimer).not.toHaveBeenCalled();
    expect(mockQueueService.removeTimerJob).not.toHaveBeenCalled();
    expect(result.reconciledCount).toBe(0);
  });
});
