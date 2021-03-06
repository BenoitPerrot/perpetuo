import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/iron-icon/iron-icon.js'
import '/node_modules/@polymer/iron-icons/iron-icons.js'

import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-deployment-request/perpetuo-deployment-state.js'

class PerpetuoDeploymentRequestEntry extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
  margin-bottom: 5px;
  border-bottom: solid 1px #ddd;
}

div.heading {
  display: flex;
  align-items: center;
  padding: 0.5em 0;
  height: 50px;
}

span {
  display: inline-block;
}

span.product-name {
  font-size: 26px;
  width: 40%;
  padding-right: 40px;
  box-sizing: border-box;
}
iron-icon {
  margin-left: 10px;
  width: 16px;
  height: 16px;
  vertical-align: bottom;
}
a:not(:hover) span.product-name iron-icon {
  display: none;
}
span.version {
  font-size: 20px;
  width: 10%;
}
span.ratio {
  vertical-align: super;
  font-size: smaller;
  color: gray;
}
span.target {
  width: 20%;
  position: relative;
}
span.target > span {
  text-overflow: ellipsis;
  white-space: nowrap;
  overflow: hidden;
  /* CSS rules are such that the width of the host is first
     computed based on its whole unconstrained content, then used
     to compute percentage-based width on children. Setting
     position to absolute removes the element selected from the
     flow, so the computation of the host width does not take into
     account the unconstrained width of this element. */
  position: absolute;
  width: 100%;
}
span.creation-date {
  width: 20%;
}
perpetuo-deployment-state {
  width: 10%;
}

a {
  text-decoration: none;
  color: initial;
}
</style>
<a href$="/deployment-requests/[[data.id]]">
  <div class="heading">
    <span class="product-name">[[data.productName]]<iron-icon icon="launch"></iron-icon></span>
    <span class="version">
      <template is="dom-repeat" items="[[shortVersion]]">
        [[item.value]]<template is="dom-if" if="[[item.ratio]]">&nbsp;<span class="ratio">[[asPercentage(item.ratio)]]%</span></template><br/>
      </template>
    </span>
    <span class="target"><span>[[computePlanLabel(data.plan)]]</span></span>
    <span class="creation-date">[[timestampToUTCDate(data.creationDate)]]</span>
    <perpetuo-deployment-state state="[[data.state]]"></perpetuo-deployment-state>
  </div>
</a>
`;
  }

  static get is() { return 'perpetuo-deployment-request-entry'; }

  static get properties() {
    return {
      data: Object,
      shortVersion: { type: Array, computed: 'computeShortVersion(data)' },
    };
  }

  computeShortVersion(data) {
    const uniformed = Array.isArray(data.version) ? data.version : [{ value: data.version }];
    return uniformed.map(version => ({
      value: (expr => expr.length > 10 ? expr.slice(0, 7) + "..." : expr)(version.value.toString()),
      ratio: version.ratio
    }));
  }

  computePlanLabel(plan) {
    return plan
          .map(planStep => planStep.name ? planStep.name : Perpetuo.Util.computeTargetExpressionLabel(planStep.targetExpression))
          .join(', ');
  }

  asPercentage(ratio) {
    return ratio * 100;
  }

  timestampToUTCDate(unixTimestamp) {
    return new Date(unixTimestamp * 1000).toISOString().replace('T', ' ').replace(/-0/g, '-').split('.')[0];
  }
}

customElements.define(PerpetuoDeploymentRequestEntry.is, PerpetuoDeploymentRequestEntry);
