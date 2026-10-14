// Delivery failures must never be interpreted as malformed input from a peer.
export class OutboundError extends Error {}

export class RateLimitError extends OutboundError {
  constructor(message, retryAt) {
    super(message);
    this.retryAt = retryAt;
  }
}

export class SocketClosedError extends OutboundError {}
