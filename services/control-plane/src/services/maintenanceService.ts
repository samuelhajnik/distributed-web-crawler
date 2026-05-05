import { RECONCILE_INTERVAL_SECONDS } from "@crawler/shared";
import { crawlReconciliationCycleDurationSeconds } from "../prometheus";
import { CrawlRunService } from "./crawlRunService";

function getIntervalMilliseconds(seconds: number): number {
  return Math.max(1, seconds) * 1000;
}

export class MaintenanceService {
  private intervalHandle: NodeJS.Timeout | null = null;

  constructor(private readonly crawlRunService: CrawlRunService) {}

  start(): void {
    this.intervalHandle = setInterval(async () => {
      const timer = crawlReconciliationCycleDurationSeconds.startTimer();
      try {
        await this.crawlRunService.runMaintenanceCycle();
      } catch (_err) {
        // keep loop alive; this is best-effort recovery
      } finally {
        timer();
      }
    }, getIntervalMilliseconds(RECONCILE_INTERVAL_SECONDS));
  }

  stop(): void {
    if (this.intervalHandle) {
      clearInterval(this.intervalHandle);
      this.intervalHandle = null;
    }
  }
}
