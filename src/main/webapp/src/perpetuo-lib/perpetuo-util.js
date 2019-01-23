export const Util = {
  isString(x) {
    return typeof x === 'string' || x instanceof String;
  },

  computeTargetExpressionLabel(e) {
    // TODO: remove once data in the DB gets rewritten <<
    if (e === '*' || (Array.isArray(e) && e.length === 1 && e[0] === '*'))
      return 'All configured targets';
    // >>
    if (Util.isString(e))
      return e;
    if (e.union) {
      const united = e.union.map(Util.computeTargetExpressionLabel);
      return united.length === 1 ? united[0] : `(${united.join(' ∪ ')})`;
    }
    if (e.intersection) {
      const intersected = e.intersection.map(Util.computeTargetExpressionLabel);
      return intersected.length === 1 ? intersected[0] : `(${intersected.join(' ∩ ')})`;
    }
    if (e.icontains) {
      return `~${e.icontains}`;
    }
    if (e.tag) {
      return e.tag;
    }
    if (Array.isArray(e)) { // TODO: remove once fully migrated to target expressions
      return e;
    }
    return 'All configured targets';
  },

  readArrayFromLocalStorage(path, maxCount) {
    const a = [];
    for (let i = 0; i < maxCount; ++i) {
      const e = window.localStorage[`${path}.${i}`];
      if (e === undefined)
        break;
      a.push(e);
    }
    return a;
  },

  writeArrayToLocalStorage(path, a) {
    a.forEach((e, i) => {
      if (e !== undefined && e !== null) {
        window.localStorage[`${path}.${i}`] = e;
      }
    });
  }
};
