import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-icon/iron-icon.js'
import '/node_modules/@polymer/iron-icons/iron-icons.js'
import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-icon-button/paper-icon-button.js'

import '../perpetuo-deployment-request/perpetuo-deployment-request-step-edition-dialog.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'

class PerpetuoDeploymentRequestStrategyEditor extends PolymerElement {

  static get template() {
    return html`
<style>
paper-icon-button,
iron-icon {
  color: var(--primary-color);
}
table {
  width: 100%;
}
td.index {
  width: 1.5em;
  text-align: right;
  padding-right: 8px;
}
td.step-definition {
  border-bottom: solid 1px #ccc;
}
span.target-summary {
  vertical-align: bottom;
  display: inline-block;
  max-width: 20em;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
paper-button#add {
  margin: 0;
  padding-left: 0
}
paper-button#add > iron-icon {
  margin-right: 8px;
}
</style>
<table>
  <template is="dom-repeat" items="[[steps]]">
    <tr>
      <td class="index">
        <span>[[inc(index)]].</span>
      </td>
      <td class="step-definition">
        <span>[[item.name]]</span>
        <span hidden="[[!item.name]]">(</span><span class="target-summary">[[computeTargetSummary(item.target)]]</span><span hidden="[[!item.name]]">)</span>
      </td>
      <td>
        <paper-icon-button icon="delete" on-tap="onDeleteStepTap"></paper-icon-button>
      </td>
    </tr>
  </template>
  <tr>
    <td></td>
    <td colspan="2">
      <paper-button id="add" on-tap="onAddStepTap"><iron-icon icon="add"></iron-icon>add step</paper-button>
    </td>
  </tr>
</table>

<perpetuo-deployment-request-step-edition-dialog product-name="[[productName]]" id="stepEditionDialog"></perpetuo-deployment-request-step-edition-dialog>
`;
  }

  static get is() { return 'perpetuo-deployment-request-strategy-editor'; }

  static get properties() {
    return {
      productName: String,

      steps: { type: Array, value: () => [], notify: true },
    }
  }

  clear() {
    this.steps = [];
  }

  inc(i) {
    return i + 1;
  }

  computeTargetSummary(target) {
    return Perpetuo.Util.computeTargetExpressionLabel(target);
  }

  onAddStepTap() {
    this.$.stepEditionDialog.clear();
    this.$.stepEditionDialog.edit().then(_ => {
      this.push('steps', _);
    });
  }

  onDeleteStepTap(e) {
    this.splice('steps', e.model.index, 1);
  }
}

customElements.define(PerpetuoDeploymentRequestStrategyEditor.is, PerpetuoDeploymentRequestStrategyEditor);
