import { IsEnum, IsNotEmpty, IsObject, IsOptional, IsString, IsUUID, Matches } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export enum RequestingServiceEnum {
  WORKFLOW_ENGINE = 'workflow-engine',
  FOLLOWUP_ENGINE = 'followup-engine',
  MEETING_ENGINE = 'meeting-engine',
}

export class TimerScheduleDto {
  @ApiPropertyOptional({ description: 'Unique event UUID for idempotency', example: 'd3b07384-d113-4a44-9c8e-aa56ebd8e011' })
  @IsOptional()
  @IsUUID()
  eventId?: string;

  @ApiProperty({ description: 'Deterministic timer key', example: 'lead:L-101:followup:step-1' })
  @IsNotEmpty()
  @IsString()
  @Matches(/^[a-zA-Z0-9_-]+(:[a-zA-Z0-9_-]+)+$/, {
    message: 'timerKey must be formatted with colon separators (e.g. lead:L-101:followup:step-1)',
  })
  timerKey: string;

  @ApiProperty({ description: 'Target execution timestamp in ISO-8601 UTC', example: '2026-08-14T12:00:00.000Z' })
  @IsNotEmpty()
  @IsString()
  targetExecutionTime: string;

  @ApiProperty({ description: 'Requesting microservice', enum: RequestingServiceEnum, example: 'followup-engine' })
  @IsNotEmpty()
  @IsEnum(RequestingServiceEnum)
  requestingService: RequestingServiceEnum;

  @ApiProperty({ description: 'Correlation ID (e.g. leadId or meetingId)', example: 'L-101' })
  @IsNotEmpty()
  @IsString()
  correlationId: string;

  @ApiProperty({ description: 'Domain-agnostic opaque payload returned byte-for-byte on execution', example: { leadId: 'L-101', step: 1 } })
  @IsNotEmpty()
  @IsObject()
  opaquePayload: Record<string, any>;
}
