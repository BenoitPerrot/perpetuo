import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-dialog/paper-dialog.js'
import '/node_modules/@polymer/paper-dialog-scrollable/paper-dialog-scrollable.js'

class PerpetuoConflictingDeploymentRequest extends PolymerElement {

  static get template() {
    return html`
<style>
.ok {
  background: #26a69a;
  color: #fff;
}
</style>
<paper-dialog id="dialog" with-backdrop>
  <h2>Conflicting deployment requests are still open</h2>
  <paper-dialog-scrollable>
    <p>This deployment request impacts targets that are being modified by on-going deployments.</p>
    <p>Wait for them to end, or close them:</p>
    <ul>
      <template is="dom-repeat" items="[[conflicts]]">
        <li><a href="/deployment-requests/[[item]]">Deployment Request #[[item]]</a></li>
      </template>
    </ul>
  </paper-dialog-scrollable>
  <div class="buttons">
    <paper-button dialog-confirm class="ok" raised>OK</paper-button>
  </div>
</paper-dialog>
`;
  }

  static get is() { return 'perpetuo-conflicting-deployment-requests'; }

  static get properties() {
    return {
      conflicts: Array
    };
  }

  open() {
    this.$.dialog.open();
  }
}

customElements.define(PerpetuoConflictingDeploymentRequest.is, PerpetuoConflictingDeploymentRequest);
