import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/marked-element/marked-element.js'

import '../perpetuo-paging/perpetuo-paging.js'

const codeToIndex = new Map(
  // Most relevant/important/informative first
  ['productFailure', 'hostFailure', 'undetermined', 'running', 'notDone', 'success'].map((e, i) => [e, i])
);

class PerpetuoTargetStatusTable extends PolymerElement {

  static get template() {
    return html`
<style>
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
</style>
<template is="dom-if" if="[[data.length]]">
  <perpetuo-paging page-sizes="[10, 50, 100]" page-size="{{pageSize}}"
                   offset="{{pageOffset}}" is-last-page="[[isLastPage(data, pageOffset, pageSize)]]"
                   count-on-page="[[pageItems.length]]" total="[[data.length]]">
    <table>
      <thead>
        <tr><th>Target</th><th>Status</th><th>Detail</th></tr>
      </thead>
      <tbody>
        <template is="dom-repeat" items="[[pageItems]]">
          <tr><td>[[item.targetAtom]]</td><td>[[item.code]]</td><td><marked-element markdown="[[item.detail]]"></marked-element></td></tr>
        </template>
      </tbody>
    </table>
  </perpetuo-paging>
</template>
`;
  }

  static get is() { return 'perpetuo-target-status-table'; }

  static get properties() {
    return {
      data: { type: Array, value: () => [] },

      pageSize: Number,
      pageOffset: Number,
      pageItems: { type: Array, computed: 'computePageItems(data, pageOffset, pageSize)' },
    }
  }

  computePageItems(data, pageOffset, pageSize) {
    return data.slice()
               .sort((a, b) => codeToIndex.get(a.code) - codeToIndex.get(b.code))
               .slice(pageOffset, pageOffset + pageSize);
  }

  isLastPage(data, pageOffset, pageSize) {
    return data.length <= pageOffset + pageSize;
  }
}

customElements.define(PerpetuoTargetStatusTable.is, PerpetuoTargetStatusTable);
