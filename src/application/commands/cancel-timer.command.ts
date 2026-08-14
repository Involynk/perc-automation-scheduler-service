export class CancelTimerCommand {
  constructor(
    public readonly eventId: string | undefined,
    public readonly timerKey: string | undefined,
    public readonly timerKeyPrefix: string | undefined,
    public readonly requestingService: string,
    public readonly reason?: string,
  ) {}
}
