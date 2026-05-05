export type RunCounts = {
  queued_count: number;
  in_progress_count: number;
  visited_count: number;
  redirect_followed_count: number;
  redirect_out_of_scope_count: number;
  redirect_301_count: number;
  forbidden_count: number;
  not_found_count: number;
  http_terminal_count: number;
  failed_count: number;
};

export type CompletionStability = {
  empty_cycles: number;
};
