import { Controller, Get, Header, HttpStatus, Inject, Res } from '@nestjs/common';
import type { Response } from 'express';
import { ReadinessProbe } from '../../infrastructure/observability/readiness-probe';
import { PrometheusMetrics } from '../../infrastructure/observability/prometheus-metrics';
import { Public } from './auth.guard';

@Public()
@Controller()
export class HealthController {
  constructor(
    @Inject(ReadinessProbe) private readonly readiness: ReadinessProbe,
    @Inject(PrometheusMetrics) private readonly metrics: PrometheusMetrics,
  ) {}

  @Get('health/live')
  live(): { status: 'ok' } {
    return { status: 'ok' };
  }

  @Get('health/ready')
  async ready(@Res({ passthrough: true }) response: Response) {
    const report = await this.readiness.check();
    response.status(report.status === 'ok' ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
    return report;
  }

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4; charset=utf-8')
  scrape(): Promise<string> {
    return this.metrics.registry.metrics();
  }
}
