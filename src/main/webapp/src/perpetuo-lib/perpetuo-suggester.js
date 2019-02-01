export class Suggester {
  static suggest(input, all, preferred) {
    const nonPreferred = all.filter(_ => !preferred.includes(_))
    const splitInput = input.split(' ');
    const normalizedInput = input.toLowerCase();
    const splitNormalizedInput = normalizedInput.split(' ');
    const matchInputIn = _ => _.reduce(
      (matches, e) => {
        if (input === e.original) {
          matches.full.push(e.original);
        }
        else if (normalizedInput === e.normalized) {
          matches.normalizedFull.push(e.original);
        }
        else if (splitInput.every(_ => e.original.includes(_))) {
          matches.partial.push(e.original);
        }
        else if (splitNormalizedInput.every(_ => e.normalized.includes(_))) {
          matches.normalizedPartial.push(e.original);
        }
        return matches;
      },
      { full: [], normalizedFull: [], partial: [], normalizedPartial: [] }
    );

    const normalize = _ => ({ original: _, normalized: _.toLowerCase() });

    const preferredMatches = matchInputIn(preferred.map(normalize));
    const nonPreferredMatches = matchInputIn(nonPreferred.map(normalize));

    return [].concat(
      preferredMatches.full,
      nonPreferredMatches.full,
      preferredMatches.normalizedFull,
      nonPreferredMatches.normalizedFull,
      preferredMatches.partial,
      preferredMatches.normalizedPartial,
      nonPreferredMatches.partial,
      nonPreferredMatches.normalizedPartial,
    );
  }
}
