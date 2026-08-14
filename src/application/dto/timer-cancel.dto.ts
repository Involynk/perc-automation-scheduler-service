import { IsEnum, IsNotEmpty, IsOptional, IsString, IsUUID } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { RequestingServiceEnum } from './timer-schedule.dto';

export class TimerCancelDto {
  @ApiPropertyOptional({ description: 'Unique event UUID for idempotency', example: 'd3b07384-d113-4a44-9c8e-aa56ebd8e011' })
  @IsOptional()
  @IsUUID()
  eventId?: string;

  @ApiPropertyOptional({ description: 'Specific timer key to cancel', example: 'lead:L-101:followup:step-1' })
  @IsOptional()
  @IsString()
  timerKey?: string;

  @ApiPropertyOptional({ description: 'Prefix pattern for bulk cancellation', example: 'lead:L-101' })
  @IsOptional()
  @IsString()
  timerKeyPrefix?: string;

  @ApiProperty({ description: 'Requesting microservice', enum: RequestingServiceEnum, example: 'followup-engine' })
  @IsNotEmpty()
  @IsEnum(RequestingServiceEnum)
  requestingService: RequestingServiceEnum;

  @ApiPropertyOptional({ description: 'Audit reason for cancellation', example: 'LEAD_REPLIED' })
  @IsOptional()
  @IsString()
  reason?: string;
}
