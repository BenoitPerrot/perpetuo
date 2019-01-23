export class LeastRecentlyUsedCache {
  constructor(maxCount, items) {
    this.maxCount = maxCount;
    this.items = (items || []).slice(0, maxCount);
  }

  insert(e) {
    const i = this.items.indexOf(e);
    if (0 <= i || this.items.length == this.maxCount) {
      this.items.splice(i, 1);
    }
    this.items.splice(0, 0, e);
    return this;
  }
}
