import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/paper-icon-button/paper-icon-button.js'
import '/node_modules/@polymer/paper-checkbox/paper-checkbox.js'

import '../perpetuo-app/perpetuo-app-toolbar.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-comment-editor.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-creation-error.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-product-selector.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-strategy-selector.js'
import '../perpetuo-deployment-request/perpetuo-deployment-request-version-selector.js'
import '../perpetuo-stepper/perpetuo-step.js'
import '../perpetuo-stepper/perpetuo-stepper.js'

class PerpetuoDeploymentRequestCreationPage extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

#stepper {
  flex: 1;
  width: 50%;
  margin-left: 64px;
}

perpetuo-app-toolbar span {
  font-family: var(--paper-font-headline_-_font-family);
  font-weight: var(--paper-font-common-base_-_font-weight);
  font-size: var(--paper-font-headline_-_font-size);
}
</style>

<perpetuo-app-toolbar>
  <div slot="title" style="display: flex; align-items: center;">
    <a href="/deployment-requests"><paper-icon-button icon="arrow-back" style="color:#fff"></paper-icon-button></a>
    <span style="margin-left: 20px;">New Deployment Request</span>
  </div>
</perpetuo-app-toolbar>

<perpetuo-stepper id="stepper" on-stepper-completed="onStepperCompleted">

  <perpetuo-step id="productSelectionStep" label="[[productSelectionStepLabel]]" complete-label="Continue" no-cancel-button
                 on-step-enter="onProductSelectionStepEnter" on-step-leave="onProductSelectionStepLeave"
                 complete-disabled="[[!selectedProductName]]">
    <perpetuo-deployment-request-product-selector slot="content" id="productSelector"
                                                  products="[[products]]" selected-product-name="{{selectedProductName}}"></perpetuo-deployment-request-product-selector>
  </perpetuo-step>

  <perpetuo-step id="versionSelectionStep" label="[[versionSelectionStepLabel]]" complete-label="Continue" cancel-label="Back"
                 on-step-enter="onVersionSelectionStepEnter" on-step-leave="onVersionSelectionStepLeave">
    <perpetuo-deployment-request-version-selector slot="content" id="versionSelector"
                                                  product-name="[[selectedProductName]]" selected-version="{{selectedVersion}}"></perpetuo-deployment-request-version-selector>
  </perpetuo-step>

  <perpetuo-step label="Select Strategy" complete-label="Continue" cancel-label="Back"
                 complete-disabled="[[!selectedStrategy.length]]">
    <perpetuo-deployment-request-strategy-selector slot="content" product-name="[[selectedProductName]]" id="strategySelector"
                                                   on-selected-strategy-changed="updateSelectedStrategy"></perpetuo-deployment-request-strategy-selector>
  </perpetuo-step>

  <perpetuo-step label="Scheduling" cancel-label="Back" complete-label="Continue"
                 on-step-enter="onSchedulingStepEnter" on-step-leave="onSchedulingStepLeave">
    <paper-checkbox id="autoRevert" slot="content">Revert automatically when the deployment fails</paper-checkbox>
  </perpetuo-step>

  <perpetuo-step label="Comment" cancel-label="Back" complete-label="Create"
                 on-step-enter="onCommentEditionStepEnter" on-step-leave="onCommentEditionStepLeave">
    <perpetuo-deployment-request-comment-editor slot="content" id="commentEditor"></perpetuo-deployment-request-comment-editor>
  </perpetuo-step>
</perpetuo-stepper>

<perpetuo-deployment-request-creation-error id="errorDialog"></perpetuo-deployment-request-creation-error>
`;
  }

  static get is() { return 'perpetuo-deployment-request-creation-page'; }

  static get properties() {
    return {
      login: String,
      active: { type: Boolean, observer: 'onActiveChanged' },
      products: { type: Array, value: () => [] },
      productSelectionStepLabel: { type: String, value: 'Select Product' },
      versionSelectionStepLabel: { type: String, value: 'Select Version' },
      selectedProductName: { type: String, observer: 'onSelectedProductNameChanged' },
      selectedVersion: String,
      selectedStrategy: { type: Array, value: () => [] },
    };
  }

  restoreDefaultValue(propertyName) {
    this[propertyName] = this.constructor.properties[propertyName].value;
  }

  onActiveChanged() {
    this.clear();
    if (this.active) {
      this.client.fetchIdentity()
          .then(login => {
            if (login) {
              this.login = login;
            } else {
              this.client.redirectToAuthorizeURL(window);
            }
          })
        this.client.fetchProducts()
          .then(products => { this.products = products; });
    }
  }

  onProductSelectionStepEnter(e) {
    this.restoreDefaultValue('productSelectionStepLabel');
    this.$.productSelector.disabled = false;
  }

  onProductSelectionStepLeave(e) {
    this.$.productSelector.disabled = true;
    this.productSelectionStepLabel = `Selected Product: ${this.selectedProductName}`;
  }

  onVersionSelectionStepEnter(e) {
    this.restoreDefaultValue('versionSelectionStepLabel');
    this.$.versionSelector.disabled = false;
  }

  onVersionSelectionStepLeave(e) {
    this.$.versionSelector.disabled = true;
    this.versionSelectionStepLabel = `Selected Version: ${this.selectedVersion}`;
  }

  onSelectedProductNameChanged(name) {
    this.$.versionSelector.clear();
    if (name) {
      this.$.productSelectionStep.fireStepCompleted();
    }
  }

  updateSelectedStrategy() {
    this.selectedStrategy = this.$.strategySelector.selectedStrategy;
  }

  onCommentEditionStepEnter(e) {
    this.$.commentEditor.disabled = false;
  }

  onCommentEditionStepLeave(e) {
    this.$.commentEditor.disabled = true;
  }

  onSchedulingStepEnter(e) {
    this.$.autoRevert.disabled = false;
  }

  onSchedulingStepLeave(e) {
    this.$.autoRevert.disabled = true;
  }

  constructor() {
    super();
    this.client = new Perpetuo.Client();
  }

  onStepperCompleted() {
    this.create();
  }

  create() {
    this.client
        .createDeploymentRequest(this.selectedProductName,
                                 this.selectedVersion,
                                 this.selectedStrategy,
                                 this.$.commentEditor.value,
                                 this.$.autoRevert.checked)
        .then(_ => _.json())
        .then(o => {
          window.location.replace(`/deployment-requests/${o.id}`);
        })
        .catch(e => {
          this.$.errorDialog.errors = e.detail.errors;
          this.$.errorDialog.open();
          this.$.stepper.reset();
        });
  }

  clear() {
    if (this.$) {
      this.$.stepper.reset();
      this.$.productSelector.clear();
      this.$.versionSelector.disabled = true;
      this.$.versionSelector.clear();
      this.$.strategySelector.clear();
      this.$.commentEditor.clear();
    }
  }
}

customElements.define(PerpetuoDeploymentRequestCreationPage.is, PerpetuoDeploymentRequestCreationPage);
