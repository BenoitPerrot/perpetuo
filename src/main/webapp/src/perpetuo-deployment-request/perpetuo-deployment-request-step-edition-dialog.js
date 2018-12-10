import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-dialog/paper-dialog.js'
import '/node_modules/@polymer/paper-dialog-scrollable/paper-dialog-scrollable.js'
import '/node_modules/@polymer/paper-input/paper-input.js'

import '../perpetuo-deployment-request/perpetuo-deployment-request-target-selector.js'

class PerpetuoDeploymentRequestStepEditionDialog extends PolymerElement {

  static get template() {
    return html`
<style>
#dialog {
  width: 80%;
}
h1 {
  margin: 0;
  padding-top: 24px;
  min-height: 40px;
  background-color: var(--primary-color);
  color: #fff;
  font-size: 22px;
  font-weight: 100;
}
</style>
<paper-dialog id="dialog" with-backdrop>
  <h1>New Deployment Step</h1>
  <paper-dialog-scrollable>
    <paper-input label="Name" value="{{name}}"></paper-input>
    <perpetuo-deployment-request-target-selector id="targetSelector" product-name="[[productName]]"
                                                 on-selected-targets-changed="updateSelectedTargets"></perpetuo-deployment-request-target-selector>
  </paper-dialog-scrollable>
  <div class="buttons">
    <paper-button id="create" dialog-confirm disabled="[[!selectedTargets]]">Add</paper-button>
    <paper-button id="cancel" dialog-dismiss>Cancel</paper-button>
  </div>
</paper-dialog>
`;
  }

  static get is() { return 'perpetuo-deployment-request-step-edition-dialog'; }

  static get properties() {
    return {
      productName: String,

      name: { type: String, value: '' },
      selectedTargets: { type: Object, value: null }
    }
  }

  clear() {
    if (this.$) {
      this.name = '';
      this.$.targetSelector.clear();
      this.selectedTargets = null;
    }
  }

  updateSelectedTargets() {
    this.selectedTargets = this.$.targetSelector.selectedTargets;
    this.$.dialog.notifyResize();
  }

  edit() {
    return new Promise((resolve, reject) => {
      const dialog = this.$.dialog;
      const onOpenedChanged = (e) => {
        if (!e.detail.value) {
          dialog.removeEventListener('opened-changed', onOpenedChanged);
          if (dialog.closingReason.confirmed) {
            resolve({ name: this.name, target: this.selectedTargets });
          } else {
            reject();
          }
        }
      };
      dialog.addEventListener('opened-changed', onOpenedChanged);
      dialog.open();
    });
  }
}

customElements.define(PerpetuoDeploymentRequestStepEditionDialog.is, PerpetuoDeploymentRequestStepEditionDialog);
