import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-dialog/paper-dialog.js'

class PerpetuoDeploymentRequestCreationError extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: block;
  top: 0;
}

h2 {
  text-transform: capitalize;
}

#dialog {
  top: 0;
}

paper-button {
  background-color: #f44336;
  color: #fff;
}
</style>
<paper-dialog id="dialog" with-backdrop>
  <h2>Cannot create deployment request</h2>
  <template is="dom-repeat" items="[[errors]]">
    <p>[[item]]</p>
  </template>
  <div class="buttons">
    <paper-button dialog-dismiss raised>ok</paper-button>
  </div>
</paper-dialog>
`;
  }

  static get is() { return 'perpetuo-deployment-request-creation-error'; }

  static get properties() {
    return {
      errors: Array
    };
  }

  open() {
    this.$.dialog.open();
  }

  close() {
    this.$.dialog.close();
  }
}

customElements.define(PerpetuoDeploymentRequestCreationError.is, PerpetuoDeploymentRequestCreationError);
