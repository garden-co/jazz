export interface SubscriptionEntry {
  uuid: string;
  id: string;
  source: string;
  resolve: string;
  status: "pending" | "loaded" | "error";
  startTime: number;
  endTime?: number;
  duration?: number;
  errorType?: string;
  callerStack?: string;
}
