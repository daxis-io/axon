// Throws during module evaluation, before any `ready` message, so the coordinator sees a boot
// failure rather than a runtime crash. This is the shape a missing or wrongly-served child chunk
// takes in production, where the host answers the worker request with something that is not
// JavaScript.
throw new Error('injected sandbox query child boot failure');
