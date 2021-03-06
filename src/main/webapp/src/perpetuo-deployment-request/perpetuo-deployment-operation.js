import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-collapse/iron-collapse.js'

import '../perpetuo-deployment-request/perpetuo-target-status-table.js'
import '../perpetuo-deployment-request/perpetuo-deployment-execution-list.js'

class PerpetuoDeploymentOperation extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

h1, h2 {
  margin: 0;
  font-weight: normal;
}
h1 {
  text-transform: capitalize;
  font-size: 24px;
}
h2 {
  font-size: 16px;
  margin-top: 1em;
}
</style>
<h1><paper-icon-button icon="[[lessOrMoreIcon(opened)]]" on-tap="toggleDetail"></paper-icon-button>[[data.kind]] Attempt #[[index]]: [[data.status]]</h1>
<iron-collapse id="detail" opened="{{opened}}">
  <p>Started by [[data.creator]] on [[timestampToDates(data.creationDate)]]</p>
  <perpetuo-target-status-table data="[[arrayifyTargetStatus(data.targetStatus)]]"></perpetuo-target-status-table>
  <h2><paper-icon-button icon="[[lessOrMoreIcon(executionDetailOpened)]]" on-tap="toggleExecutionDetail"></paper-icon-button>Execution Detail</h2>
  <iron-collapse id="executionDetail" opened="{{executionDetailOpened}}">
    <perpetuo-deployment-execution-list data="[[data.executions]]"></perpetuo-deployment-execution-list>
  </iron-collapse>
</iron-collapse>
`;
  }

  static get is() { return 'perpetuo-deployment-operation'; }

  static get properties() {
    return {
      index: { type: Number },
      data: Object,
      opened: { type: Boolean, notify: true, reflectToAttribute: true },
      executionDetailOpened: { type: Boolean, value: false }
    };
  }

  timestampToDates(t) {
    const date = new Date(t * 1000);

    const localYMD = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate();
    const localTime = date.toTimeString().split(' ')[0];

    const iso = date.toISOString().split('T');
    const isoYMD = iso[0].replace(/-0/g, '-');
    const isoTime = iso[1].split('.')[0];

    return `${isoYMD} at ${isoTime} UTC (` + (localYMD !== isoYMD ? localYMD + ' ' : '') + localTime + ' local time)';
  }

  arrayifyTargetStatus(targetStatusMap) {
    return Object.keys(targetStatusMap).map(targetAtom => {
      const targetAtomStatus = targetStatusMap[targetAtom];
      return { targetAtom: targetAtom, code: targetAtomStatus.code, detail: targetAtomStatus.detail };
    });
  }

  toggleDetail() {
    this.$.detail.toggle();
  }

  toggleExecutionDetail() {
    this.$.executionDetail.toggle();
  }

  lessOrMoreIcon(opened) {
    return opened ? 'expand-less' : 'expand-more';
  }
}

customElements.define(PerpetuoDeploymentOperation.is, PerpetuoDeploymentOperation);
