export class RescheduleTimerCommand {
  constructor(
    public readonly eventId: string | undefined,
    public readonly timerKey: string,
    public readonly newTargetExecutionTime: Date,
    public readonly requestingService: string,
    public readonly correlationId: string,
    public readonly updatedOpaquePayload?: Record<string, any>,
  ) {}
}
