import { Test, TestingModule } from '@nestjs/testing';
import { TimerService } from './timer.service';
import { PrismaTimerRepository } from '../../infrastructure/persistence/prisma-timer.repository';
import { TimerQueueService } from '../../infrastructure/bullmq/timer-queue.service';
import { ScheduleTimerCommand } from '../commands/schedule-timer.command';
import { CancelTimerCommand } from '../commands/cancel-timer.command';
import { RescheduleTimerCommand } from '../commands/reschedule-timer.command';
import { TimerStatus } from '../../domain/enums/timer-status.enum';

describe('TimerService (Unit Tests)', () => {
  let service: TimerService;
  let mockTimerRepo: Partial<PrismaTimerRepository>;
  let mockQueueService: Partial<TimerQueueService>;

  beforeEach(async () => {
    mockTimerRepo = {
      createOrUpsertPending: jest.fn().mockResolvedValue({
        timer: {
          id: 'timer-123',
          timerKey: 'lead:L-101:followup:step-1',
          requestingService: 'followup-engine',
          correlationId: 'L-101',
          targetExecutionTime: new Date(Date.now() + 7200000),
          status: TimerStatus.PENDING,
          opaquePayload: { step: 1 },
          retryCount: 0,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        isDuplicate: false,
      }),
      cancelByKey: jest.fn().mockResolvedValue({
        cancelledKeys: ['lead:L-101:followup:step-1'],
        isDuplicate: false,
      }),
      cancelByPrefix: jest.fn().mockResolvedValue({
        cancelledKeys: ['lead:L-101:followup:step-1', 'lead:L-101:escalation'],
        isDuplicate: false,
      }),
      rescheduleByKey: jest.fn().mockResolvedValue({
        timer: {
          id: 'timer-123',
          timerKey: 'meeting:M-88:reminder',
          requestingService: 'meeting-engine',
          correlationId: 'M-88',
          targetExecutionTime: new Date(Date.now() + 10000000),
          status: TimerStatus.PENDING,
          opaquePayload: { type: 'DEMO' },
          retryCount: 0,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        isDuplicate: false,
      }),
      findByKey: jest.fn(),
    };

    mockQueueService = {
      addDelayedTimer: jest.fn().mockResolvedValue(undefined),
      removeTimerJob: jest.fn().mockResolvedValue(true),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TimerService,
        { provide: PrismaTimerRepository, useValue: mockTimerRepo },
        { provide: TimerQueueService, useValue: mockQueueService },
      ],
    }).compile();

    service = module.get<TimerService>(TimerService);
  });

  it('should schedule a timer and enqueue delayed job in BullMQ', async () => {
    const futureDate = new Date(Date.now() + 7200000);
    const command = new ScheduleTimerCommand(
      'event-uuid-1',
      'lead:L-101:followup:step-1',
      futureDate,
      'followup-engine',
      'L-101',
      { step: 1 },
    );

    const result = await service.scheduleTimer(command);

    expect(mockTimerRepo.createOrUpsertPending).toHaveBeenCalledWith(
      {
        timerKey: 'lead:L-101:followup:step-1',
        requestingService: 'followup-engine',
        correlationId: 'L-101',
        targetExecutionTime: futureDate,
        opaquePayload: { step: 1 },
      },
      'event-uuid-1',
    );
    expect(mockQueueService.addDelayedTimer).toHaveBeenCalled();
    expect(result.timerKey).toBe('lead:L-101:followup:step-1');
    expect(result.status).toBe(TimerStatus.PENDING);
  });

  it('should cancel a timer by key and remove BullMQ job', async () => {
    const command = new CancelTimerCommand(
      'event-uuid-2',
      'lead:L-101:followup:step-1',
      undefined,
      'followup-engine',
      'LEAD_REPLIED',
    );

    const result = await service.cancelTimer(command);

    expect(mockTimerRepo.cancelByKey).toHaveBeenCalledWith('lead:L-101:followup:step-1', 'LEAD_REPLIED', 'event-uuid-2');
    expect(mockQueueService.removeTimerJob).toHaveBeenCalledWith('lead:L-101:followup:step-1');
    expect(result.status).toBe('CANCELLED');
    expect(result.cancelledKeys).toContain('lead:L-101:followup:step-1');
  });

  it('should cancel timers by prefix and remove all matching BullMQ jobs', async () => {
    const command = new CancelTimerCommand(
      'event-uuid-3',
      undefined,
      'lead:L-101',
      'workflow-engine',
      'ADMISSION_COMPLETED',
    );

    const result = await service.cancelTimer(command);

    expect(mockTimerRepo.cancelByPrefix).toHaveBeenCalledWith('lead:L-101', 'ADMISSION_COMPLETED', 'event-uuid-3');
    expect(mockQueueService.removeTimerJob).toHaveBeenCalledTimes(2);
    expect(result.cancelledKeys.length).toBe(2);
  });

  it('should reschedule an existing timer to a future time', async () => {
    const newTarget = new Date(Date.now() + 10000000);
    const command = new RescheduleTimerCommand(
      'event-uuid-4',
      'meeting:M-88:reminder',
      newTarget,
      'meeting-engine',
      'M-88',
      { type: 'DEMO' },
    );

    const result = await service.rescheduleTimer(command);

    expect(mockTimerRepo.rescheduleByKey).toHaveBeenCalled();
    expect(mockQueueService.addDelayedTimer).toHaveBeenCalled();
    expect(result.timerKey).toBe('meeting:M-88:reminder');
  });
});
