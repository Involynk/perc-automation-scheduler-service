export class ScheduleTimerCommand {
  constructor(
    public readonly eventId: string | undefined,
    public readonly timerKey: string,
    public readonly targetExecutionTime: Date,
    public readonly requestingService: string,
    public readonly correlationId: string,
    public readonly opaquePayload: Record<string, any>,
  ) {}
}
