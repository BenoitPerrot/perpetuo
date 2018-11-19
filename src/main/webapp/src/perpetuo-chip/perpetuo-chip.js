import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'
import { mixinBehaviors } from '/node_modules/@polymer/polymer/lib/legacy/class.js';

import { IronControlState } from '/node_modules/@polymer/iron-behaviors/iron-control-state.js'
import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/paper-icon-button/paper-icon-button.js'

class PerpetuoChip extends mixinBehaviors([IronControlState], PolymerElement) {

  static get template() {
    return html`
<style>
:host {
  margin: 4px 4px 4px 0;
  display: inline-flex;
  flex-direction: row;
  align-items: center;
  border-radius: 32px;
  background-color: #e0dee0;
  align-items: center;
  @apply --perpetuo-chip;
}
:host > span {
  margin: 8px -4px 8px 12px;
}
:host > paper-icon-button {
  color: #9e9c9e;
  min-width: 40px;
  min-height: 40px;
  @apply --perpetuo-chip-button;
}
:host(:hover) {
  background-color: #767476;
  color: #fff;
  @apply --perpetuo-chip-hover;
}
:host(:hover) > paper-icon-button {
  color: #fff;
  @apply --perpetuo-chip-button-hover;
}
:host([non-deletable]) > paper-icon-button {
  display: none;
}
:host([non-deletable]) > span {
  margin-right: 12px;
}
</style>
<span>[[label]]</span>
<paper-icon-button disabled$="[[disabled]]" icon="cancel" on-tap="fireDeleteTap"></paper-icon-button>
`;
  }

  static get is() { return 'perpetuo-chip'; }

  static get properties() {
    return {
      label: String
    }
  }

  fireDeleteTap() {
    this.dispatchEvent(new CustomEvent('delete-tap'));
  }
}

customElements.define(PerpetuoChip.is, PerpetuoChip);
