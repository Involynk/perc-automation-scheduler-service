import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { TimerStatus } from '../../domain/enums/timer-status.enum';

export class TimerResponseDto {
  @ApiProperty({ description: 'Internal Timer UUID', example: 'd3b07384-d113-4a44-9c8e-aa56ebd8e011' })
  id: string;

  @ApiProperty({ description: 'Deterministic timer key', example: 'lead:L-101:followup:step-1' })
  timerKey: string;

  @ApiProperty({ description: 'Requesting service', example: 'followup-engine' })
  requestingService: string;

  @ApiProperty({ description: 'Correlation ID', example: 'L-101' })
  correlationId: string;

  @ApiProperty({ description: 'Target execution timestamp', example: '2026-08-14T12:00:00.000Z' })
  targetExecutionTime: string;

  @ApiProperty({ description: 'Current timer status', enum: TimerStatus, example: TimerStatus.PENDING })
  status: TimerStatus;

  @ApiProperty({ description: 'Opaque JSON payload' })
  opaquePayload: Record<string, any>;

  @ApiPropertyOptional({ description: 'Execution timestamp' })
  executedAt?: string;

  @ApiPropertyOptional({ description: 'Cancellation timestamp' })
  cancelledAt?: string;

  @ApiPropertyOptional({ description: 'Cancellation reason' })
  cancelReason?: string;

  @ApiProperty({ description: 'Creation timestamp' })
  createdAt: string;

  @ApiProperty({ description: 'Last update timestamp' })
  updatedAt: string;
}

export class CancelResponseDto {
  @ApiProperty({ description: 'Cancellation status', example: 'CANCELLED' })
  status: string;

  @ApiProperty({ description: 'List of cancelled timer keys', example: ['lead:L-101:followup:step-1'] })
  cancelledKeys: string[];

  @ApiPropertyOptional({ description: 'Cancellation reason', example: 'LEAD_REPLIED' })
  reason?: string;
}
