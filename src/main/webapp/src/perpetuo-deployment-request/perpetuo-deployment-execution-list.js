import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/marked-element/marked-element.js'

class PerpetuoDeploymentExecutionList extends PolymerElement {

  static get template() {
    return html`<style>
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
  color: hsl(0,0%,13%);
}
table tr {
  height: 42px;
  border-bottom: solid 1px #ddd;
}
table tr td:first-child {
  padding-left: 24px;
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

tbody td::first-letter {
  text-transform: uppercase;
}
</style>
<template is="dom-if" if="[[data.length]]">
  <table>
    <thead>
    <tr>
      <th>Execution state</th>
      <th>Detail</th>
    </tr>
    </thead>
    <tbody>
    <template is="dom-repeat" items="[[data]]">
      <tr>
        <td>
          [[humanReadable(item.state)]]
          <template is="dom-if" if="[[item.href]]">
            (<a href$="[[item.href]]">technical logs</a>)
          </template>
        </td>
        <td>
          <marked-element markdown="[[item.detail]]"></marked-element>
        </td>
      </tr>
    </template>
    </tbody>
  </table>
</template>
`;
  }

  static get is() { return 'perpetuo-deployment-execution-list'; }

  static get properties() {
    return {
      data: { type: Array, value: () => [] },
    }
  }

  humanReadable(actionName) {
    return actionName.replace('initFailed', 'nothing happened').replace(/([A-Z])/g, ' $1').toLowerCase();
  }
}

customElements.define(PerpetuoDeploymentExecutionList.is, PerpetuoDeploymentExecutionList);
