import { IsEnum, IsNotEmpty, IsObject, IsOptional, IsString, IsUUID } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { RequestingServiceEnum } from './timer-schedule.dto';

export class TimerRescheduleDto {
  @ApiPropertyOptional({ description: 'Unique event UUID for idempotency', example: 'd3b07384-d113-4a44-9c8e-aa56ebd8e011' })
  @IsOptional()
  @IsUUID()
  eventId?: string;

  @ApiProperty({ description: 'Existing timer key to reschedule', example: 'meeting:M-88:reminder' })
  @IsNotEmpty()
  @IsString()
  timerKey: string;

  @ApiProperty({ description: 'New target execution timestamp in ISO-8601 UTC', example: '2026-08-16T14:00:00.000Z' })
  @IsNotEmpty()
  @IsString()
  newTargetExecutionTime: string;

  @ApiProperty({ description: 'Requesting microservice', enum: RequestingServiceEnum, example: 'meeting-engine' })
  @IsNotEmpty()
  @IsEnum(RequestingServiceEnum)
  requestingService: RequestingServiceEnum;

  @ApiProperty({ description: 'Correlation ID', example: 'M-88' })
  @IsNotEmpty()
  @IsString()
  correlationId: string;

  @ApiPropertyOptional({ description: 'Optional replacement opaque payload', example: { meetingId: 'M-88', rescheduled: true } })
  @IsOptional()
  @IsObject()
  updatedOpaquePayload?: Record<string, any>;
}
