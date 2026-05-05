export type ControlPlaneConfig = {
  port: number;
};

export function loadConfig(): ControlPlaneConfig {
  return {
    port: Number(process.env.CONTROL_PLANE_PORT ?? 3000)
  };
}
