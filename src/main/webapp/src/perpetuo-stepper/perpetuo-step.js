import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-collapse/iron-collapse.js'
import '/node_modules/@polymer/paper-button/paper-button.js'

class PerpetuoStep extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  margin: 0 24px;
  display: block;
}
.header {
  display: flex;
  align-items: center;
  @apply --perpetuo-step-header;
}
.header .round {
  position: relative;
  display: inline-flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  border-radius: 50%;
  min-height: 30px;
  min-width: 30px;
  font-size: 16px;
  margin: 8px 8px 8px 0;

  background-color: hsl(0,0%,38%);
  color: #fff;
}
:host([active]) .header .round {
  background-color: var(--perpetuo-blue);
  color: #fff;
}
:host([completed]) .header .round {
  background-color: var(--perpetuo-blue);
  font-weight: bold;
  font-size: 110%;
  color: #fff;
}
:host([completed]) .header .round span {
  display: none;
}

.header .round #checkmark {
  position: absolute;
  width: 20%;
  height: 40%;
  border-style: solid;
  border-top: none;
  border-left: none;
  border-right-width: 2px;
  border-bottom-width: 2px;
  border-color: #fff;
  transform-origin: 75% 50%;
  box-sizing: content-box;
}
:host(:not([completed])) .header .round #checkmark {
  display: none;
}
:host([completed]) .header .round #checkmark {
  animation: checkmark-expand 140ms ease-out forwards;
}
@keyframes checkmark-expand {
  0% {
    transform: scale(0, 0) rotate(45deg);
  }
  100% {
    transform: scale(1, 1) rotate(45deg);
  }
}

.header .label {
  font-weight: 100;
  color: hsl(0,0%,87%);
}
:host([active]) .header .label {
  color: hsl(0,0%,38%);
  font-weight: 500;
}
:host([completed]) .header .label {
  color: hsl(0,0%,38%);
}

.container {
  display: flex;
}
.container .vertical-bar {
  display: flex;
  width: 15px;
  border-right: solid 1px #bdbdbd;
  margin-right: 12px;
  min-height: 24px;
}
:host(:last-of-type) .container .vertical-bar {
  border: none;
}
.container > iron-collapse {
  width: 100%;
}
.container .content {
  padding-left: 8px;
  padding-bottom: 24px;
  flex: 1;
}

.buttons {
  margin-top: 16px;
}
:host([no-buttons]) .buttons {
  display: none;
}
:host([no-complete-button]) #complete {
  visibility: hidden;
}
:host([no-cancel-button]) #cancel {
  visibility: hidden;
}

paper-button:not([disabled]).blue {
  background: var(--perpetuo-blue);
  color: #fff;
}
paper-button:not([disabled]).green {
  background: #26a69a;
  color: #fff;
}
paper-button:not([disabled]).red {
  background-color: var(--perpetuo-red);
  color: #fff;
}
paper-button:not([disabled]).white {
  background-color: #fff;
  color: hsl(0,0%,40%);
}
</style>
<div class="header">
  <div class="round"><span>[[index]]</span><div id="checkmark"></div></div>
  <slot name="header">
    <span class="label">[[label]]</span>
  </slot>
</div>
<div class="container">
  <div class="vertical-bar"></div>
  <iron-collapse opened="[[active]]">
    <div class="content">
      <slot name="content"></slot>
      <div class="buttons">
        <paper-button id="complete" disabled="[[completeDisabled]]" on-tap="fireStepCompleted" class="blue" id="complete">[[completeLabel]]</paper-button>
        <paper-button id="cancel" on-tap="fireStepCancelled" class="white" id="cancel">[[cancelLabel]]</paper-button>
      </div>
    </div>
  </iron-collapse>
</div>
`;
  }

  static get is() { return 'perpetuo-step'; }

  static get properties() {
    return {
      index: Number,
      label: String,
      completeLabel: { type: String, value: 'Complete' },
      cancelLabel: { type: String, value: 'Cancel' },
      active: { type: Boolean, value: false, notify: true, reflectToAttribute: true },
      disabled: { type: Boolean, value: false, notify: true, reflectToAttribute: true },
      completeDisabled: { type: Boolean, value: false, notify: true },
      completed: { type: Boolean, value: false, notify: true, reflectToAttribute: true }
    };
  }

  fireStepCompleted() {
    this.completed = true;
    this.dispatchEvent(new CustomEvent('step-completed'));
  }

  fireStepCancelled() {
    this.completed = false;
    this.dispatchEvent(new CustomEvent('step-cancelled'));
  }
}

customElements.define(PerpetuoStep.is, PerpetuoStep);
