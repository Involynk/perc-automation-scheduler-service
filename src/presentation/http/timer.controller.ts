import {
  Controller,
  Post,
  Delete,
  Get,
  Body,
  Param,
  Query,
  HttpStatus,
  HttpCode,
  NotFoundException,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery } from '@nestjs/swagger';
import { TimerService } from '../../application/services/timer.service';
import { TimerScheduleDto } from '../../application/dto/timer-schedule.dto';
import { TimerCancelDto } from '../../application/dto/timer-cancel.dto';
import { TimerRescheduleDto } from '../../application/dto/timer-reschedule.dto';
import { TimerResponseDto, CancelResponseDto } from '../../application/dto/timer-response.dto';
import { ScheduleTimerCommand } from '../../application/commands/schedule-timer.command';
import { CancelTimerCommand } from '../../application/commands/cancel-timer.command';
import { RescheduleTimerCommand } from '../../application/commands/reschedule-timer.command';

@ApiTags('Timers')
@Controller('api/v1/timers')
export class TimerController {
  constructor(private readonly timerService: TimerService) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({ summary: 'Schedule a new timer (Synchronous API)' })
  @ApiResponse({ status: 201, description: 'Timer scheduled successfully', type: TimerResponseDto })
  async schedule(@Body() dto: TimerScheduleDto): Promise<TimerResponseDto> {
    const targetTime = new Date(dto.targetExecutionTime);
    const command = new ScheduleTimerCommand(
      dto.eventId,
      dto.timerKey,
      targetTime,
      dto.requestingService,
      dto.correlationId,
      dto.opaquePayload,
    );
    return this.timerService.scheduleTimer(command);
  }

  @Delete(':timerKey')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Cancel an active timer by key' })
  @ApiParam({ name: 'timerKey', description: 'Deterministic timer key (e.g. lead:L-101:followup:step-1)' })
  @ApiQuery({ name: 'reason', required: false, description: 'Audit reason for cancellation' })
  @ApiResponse({ status: 200, description: 'Timer cancelled successfully', type: CancelResponseDto })
  async cancel(
    @Param('timerKey') timerKey: string,
    @Query('reason') reason?: string,
  ): Promise<CancelResponseDto> {
    const command = new CancelTimerCommand(undefined, timerKey, undefined, 'REST_CLIENT', reason);
    return this.timerService.cancelTimer(command);
  }

  @Post('cancel')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Cancel single or bulk timers (Prefix or Key)' })
  @ApiResponse({ status: 200, description: 'Timers cancelled successfully', type: CancelResponseDto })
  async cancelBatch(@Body() dto: TimerCancelDto): Promise<CancelResponseDto> {
    const command = new CancelTimerCommand(
      dto.eventId,
      dto.timerKey,
      dto.timerKeyPrefix,
      dto.requestingService,
      dto.reason,
    );
    return this.timerService.cancelTimer(command);
  }

  @Post('reschedule')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Reschedule an existing timer' })
  @ApiResponse({ status: 200, description: 'Timer rescheduled successfully', type: TimerResponseDto })
  async reschedule(@Body() dto: TimerRescheduleDto): Promise<TimerResponseDto> {
    const newTargetTime = new Date(dto.newTargetExecutionTime);
    const command = new RescheduleTimerCommand(
      dto.eventId,
      dto.timerKey,
      newTargetTime,
      dto.requestingService,
      dto.correlationId,
      dto.updatedOpaquePayload,
    );
    return this.timerService.rescheduleTimer(command);
  }

  @Get(':timerKey')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Get timer details by key' })
  @ApiParam({ name: 'timerKey', description: 'Deterministic timer key' })
  @ApiResponse({ status: 200, description: 'Timer details', type: TimerResponseDto })
  @ApiResponse({ status: 404, description: 'Timer not found' })
  async getByKey(@Param('timerKey') timerKey: string): Promise<TimerResponseDto> {
    const timer = await this.timerService.getTimerByKey(timerKey);
    if (!timer) {
      throw new NotFoundException(`Timer with key ${timerKey} not found.`);
    }
    return timer;
  }
}
