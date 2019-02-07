import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-dropdown/iron-dropdown.js'
import '/node_modules/@polymer/paper-item/paper-item.js'
import '/node_modules/@polymer/paper-listbox/paper-listbox.js'
import '/node_modules/@polymer/paper-styles/shadow.js'

import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'

class PerpetuoSuggestionDropdown extends PolymerElement {
  static get template() {
    return html`
<style>
#dropdown {
  @apply --shadow-elevation-2dp;
  cursor: pointer;
}
</style>
<iron-dropdown id="dropdown" no-auto-focus no-cancel-on-outside-click>
  <paper-listbox id="listbox" slot="dropdown-content" on-selected-changed="onSelectedChanged">
    <template is="dom-repeat" items="[[suggestions]]">
      <paper-item>[[item]]</paper-item>
    </template>
  </paper-listbox>
</iron-dropdown>
`;
  }

  static get is() { return 'perpetuo-suggestion-dropdown'; }

  static get properties() {
    return {
      choices: Array,
      maxCount: { type: Number, value: Number.POSITIVE_INFINITY },
      lruPath: String,
      filter: String,
      suggestions: { type: Array, value: () => [], observer: 'onSuggestionsChanged' },
    };
  }

  static get observers() {
    return [
      'computeSuggestions(choices, maxCount, filter)'
    ];
  }

  computeSuggestions(choices, maxCount, filter) {
    const preferred = this.lruPath ? Perpetuo.Util.readArrayFromLocalStorage(this.lruPath, maxCount) : choices;
    this.suggestions =
      (filter ? Perpetuo.Suggester.suggest(filter, choices, preferred) : preferred).slice(0, this.maxCount);
  }

  onSuggestionsChanged() {
    this.$.dropdown.notifyResize();
  }

  onSelectedChanged(e) {
    const selectedSuggestion = e.detail.value === null ? null : this.suggestions[e.detail.value];
    if (selectedSuggestion && this.lruPath) {
      Perpetuo.Util.writeArrayToLocalStorage(
        this.lruPath,
        new Perpetuo.LeastRecentlyUsedCache(this.maxCount, Perpetuo.Util.readArrayFromLocalStorage(this.lruPath, this.maxCount))
          .insert(selectedSuggestion)
          .items
      );
    }
    this.dispatchEvent(new CustomEvent('suggestion-selected', {detail: {value: selectedSuggestion}}));
  }

  open() {
    this.computeSuggestions(this.choices, this.maxCount, this.filter);
    this.$.dropdown.open();
  }

  close() {
    this.$.dropdown.close();
  }

  focus() {
    this.$.listbox.focus();
  }

  select(e) {
    const i = this.suggestions.findIndex(_ => _ == e);
    if (0 <= i) {
      this.$.listbox.selected = i;
    }
  }

  unselect() {
    this.$.listbox.select(null);
  }
}

customElements.define(PerpetuoSuggestionDropdown.is, PerpetuoSuggestionDropdown);
