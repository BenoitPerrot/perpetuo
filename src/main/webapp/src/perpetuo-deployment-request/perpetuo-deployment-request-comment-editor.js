import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-input/paper-textarea.js'

class PerpetuoDeploymentRequestCommentEditor extends PolymerElement {

  static get template() {
    return html`
<paper-textarea no-label-float id="main" disabled="[[disabled]]"></paper-textarea>
`;
  }

  static get is() { return 'perpetuo-deployment-request-comment-editor'; }

  static get properties() {
    return {
      disabled: Boolean,
    }
  }

  get value() {
    return this.$.main.value;
  }

  clear() {
    if (this.$) {
      this.$.main.value = '';
    }
  }
}

customElements.define(PerpetuoDeploymentRequestCommentEditor.is, PerpetuoDeploymentRequestCommentEditor);
