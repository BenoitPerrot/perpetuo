import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '../perpetuo-suggestion-dropdown/perpetuo-suggestion-dropdown.js'

class PerpetuoDeploymentRequestFilterEditor extends PolymerElement {
  static get is() {
    return 'perpetuo-deployment-request-filter-editor';
  }

  static get template() {
    return html`
<style>
:host {
  display: block;
}

input {
  margin-left: 0.5em;
  width: 100%;

  border: none;
  outline: none;

  font-family: var(--paper-font-headline_-_font-family);
  font-weight: var(--paper-font-common-base_-_font-weight);
  font-size: var(--paper-font-headline_-_font-size);
  background-color: var(--perpetuo-blue);
  color: #fff;
}
input::placeholder {
  color: #fff;
}
input:focus::placeholder {
  opacity: 0.5;
}
</style>
<input id="input" on-focus="onInputFocus" on-keydown="onInputKeyDown" value="{{filter::input}}"/>
<div>
  <perpetuo-suggestion-dropdown id="suggestionDropdown" choices="[[productNames]]" lru-path="productSelector.lru" max-count="10" filter="[[filter]]" on-suggestion-selected="onSuggestionSelected"></perpetuo-suggestion-dropdown>
</div>
    `;
  }

  static get properties() {
    return {
      productNames: Array,
      focused: { type: Boolean, reflectToAttribute: true, notify: true },
      filter: { type: String, notify: true }
    }
  }

  ready() {
    super.ready();

    this.addEventListener('blur', () => {
      this.focused = false;
      this.$.suggestionDropdown.close();
    });
    this.addEventListener('focus', () => this.focus());
  }

  focus() {
    this.focused = true;
    this.$.input.focus();
    this.$.input.select();
  }

  onInputFocus() {
    this.$.suggestionDropdown.open();
  }

  onInputKeyDown(e) {
    if (e.key !== 'Escape') {
      this.$.suggestionDropdown.open();
    }
  }

  onSuggestionSelected(e) {
    if (e.detail.value) {
      this.filter = e.detail.value;
      this.dispatchEvent(new CustomEvent('product-name-selected', {detail: {value: e.detail.value}}));
      this.$.suggestionDropdown.select(null); // TODO: drop the "selection" concept from the suggestion dropdown
      this.$.suggestionDropdown.close();
      this.focused = false;
    }
  }

  select(name) {
    return this.$.suggestionDropdown.select(name);
  }
}

customElements.define(PerpetuoDeploymentRequestFilterEditor.is, PerpetuoDeploymentRequestFilterEditor);
