export function AsyncRepeater(f, delayInMs) {
  let suspended = true;
  let timeoutId = -1;

  const invokeThenRepeat = () =>
    f().then(repeatAfterDelay, repeatAfterDelay);
  const repeatAfterDelay = () => {
    if (!suspended)
      timeoutId = setTimeout(invokeThenRepeat, delayInMs);
  }

  this.suspend = () => {
    suspended = true;
    clearTimeout(timeoutId);
    return this;
  };
  this.resume = () => {
    suspended = false;
    invokeThenRepeat();
    return this;
  };
  this.fastForward = () => {
    if (!suspended) {
      clearTimeout(timeoutId);
      invokeThenRepeat();
    }
    return this;
  };
  Object.defineProperty(this, 'suspended', {
    set: (value) => value ? this.suspend() : this.resume(),
    get: () => suspended
  });
}
