import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-dialog/paper-dialog.js'
import '/node_modules/@polymer/paper-dialog-scrollable/paper-dialog-scrollable.js'
import '/node_modules/@polymer/paper-input/paper-input.js'

class PerpetuoRevertPlanDialog extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

.cancel {
  background-color: var(--perpetuo-red);
  color: #fff;
}
.revert {
  background: #26a69a;
  color: #fff;
}

table {
  width: 100%;
  border-collapse: collapse;
}
table th {
  font-weight: 500;
  color: #757575;
  text-align: left;
}
table td {
  color: hsl(0, 0%, 13%);
}
table tr {
  height: 42px;
  border-bottom: solid 1px #ddd;
}
table tbody tr:last-of-type {
  border: 0;
}
table td:not(:last-child) {
  padding-right: 56px;
}
table tbody tr:hover {
  background-color: #eee;
}
</style>
<paper-dialog id="dialog" with-backdrop opened="{{opened}}">
  <h2>Revert plan</h2>
  <template is="dom-if" if="[[determined.length]]">
    <p>The following versions will be applied:</p>
    <paper-dialog-scrollable>
      <table>
        <thead>
          <tr>
            <th>Version</th>
            <th>Targets</th>
          </tr>
        </thead>
        <tbody>
          <template is="dom-repeat" items="[[determined]]">
            <tr>
              <td>
                <template is="dom-repeat" items="[[computeVersion(item)]]">
                  <template is="dom-if" if="[[index]]">+</template>
                  [[item.value]]
                  <template is="dom-if" if="[[item.ratio]]">&nbsp;<span class="ratio">[[asPercentage(item.ratio)]]%</span></template>
                </template>
              </td>
              <td>[[sumUpArray(item.targetAtoms)]]</td>
            </tr>
          </template>
        </tbody>
      </table>
    </paper-dialog-scrollable>
  </template>
  <template is="dom-if" if="[[undetermined.length]]">
    <p>Some impacted targets have no previous version ([[sumUpArray(undetermined)]]).</p>
    <p>Please specify the version to use for them:
      <paper-input label="Default version" value="{{defaultVersion}}"></paper-input>
    </p>
  </template>
  <div class="buttons">
    <paper-button dialog-dismiss class="cancel" raised>Cancel</paper-button>
    <paper-button dialog-confirm class="revert" raised>Revert</paper-button>
  </div>
</paper-dialog>
`;
  }

  static get is() { return 'perpetuo-revert-plan-dialog'; }

  static get properties() {
    return {
      opened: { type: Boolean, notify: true },
      determined: Array,
      undetermined: Array,
      defaultVersion: String,
    }
  }

  open(revertPlan) {
    this.defaultVersion = undefined;
    this.determined = revertPlan.determined;
    this.undetermined = revertPlan.undetermined;
    this.$.dialog.open();
  }

  get closingReason() {
    return this.$.dialog.closingReason;
  }

  computeVersion(data) {
    return Array.isArray(data.version) ? data.version : [{ value: data.version }];
  }

  asPercentage(ratio) {
    return ratio * 100;
  }

  sumUpArray(arr) {
    const sorted = arr.sort();
    return (sorted.length < 8 ?
            sorted :
            sorted.slice(0, 3).concat(["..."], sorted.slice(-3))
    ).join(", ");
  }
}

customElements.define(PerpetuoRevertPlanDialog.is, PerpetuoRevertPlanDialog);
