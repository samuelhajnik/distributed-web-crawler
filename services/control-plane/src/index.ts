import { pgPool, redisConnection } from "@crawler/shared";
import { loadConfig } from "./config";
import { createServer } from "./server";
import { logCp } from "./logging";
import { CrawlRunService } from "./services/crawlRunService";
import { MaintenanceService } from "./services/maintenanceService";

const config = loadConfig();
const crawlRunService = new CrawlRunService();
const maintenanceService = new MaintenanceService(crawlRunService);
const app = createServer(crawlRunService);

maintenanceService.start();

app.listen(config.port, () => {
  process.stdout.write(`[component=control-plane] listening on :${config.port} metrics=/metrics\n`);
});

process.on("SIGINT", async () => {
  maintenanceService.stop();
  await crawlRunService.close();
  await redisConnection.quit();
  await pgPool.end();
  logCp(undefined, "shutdown complete");
  process.exit(0);
});
