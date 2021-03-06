import { PolymerElement, html } from '/node_modules/@polymer/polymer/polymer-element.js'

import '/node_modules/@polymer/marked-element/marked-element.js'
import '/node_modules/@polymer/paper-card/paper-card.js'
import '/node_modules/@polymer/paper-button/paper-button.js'
import '/node_modules/@polymer/paper-dialog/paper-dialog.js'
import '/node_modules/@polymer/paper-dialog-scrollable/paper-dialog-scrollable.js'
import '/node_modules/@polymer/paper-input/paper-input.js'
import '/node_modules/@polymer/paper-progress/paper-progress.js'

import '../perpetuo-app/perpetuo-app-toolbar.js'
import Perpetuo from '../perpetuo-lib/perpetuo-lib.js'
import '../perpetuo-deployment-request/perpetuo-conflicting-deployment-requests.js'
import '../perpetuo-deployment-request/perpetuo-deployment-operation.js'
import '../perpetuo-deployment-request/perpetuo-deployment-state.js'
import '../perpetuo-deployment-request/perpetuo-revert-plan-dialog.js'

function computePlanStepLabel(planStep) {
  return planStep.name || Perpetuo.Util.computeTargetExpressionLabel(planStep.targetExpression);
}

function computeOperationGroupLabel(planStepIds, plan) {
  return planStepIds
        .map(id => plan.find(_ => _.id === id))
        .map(planStep => computePlanStepLabel(planStep))
        .join(', ');
}

class PerpetuoDeploymentRequest extends PolymerElement {

  static get template() {
    return html`
<style>
:host {
  display: flex;
  flex-direction: column;
}

.body {
  display: flex;
  flex-direction: column;
  padding: 20px;
}

.body[hidden] {
  opacity: 0;
}
.body:not([hidden]) {
  opacity: 1;
  transition: opacity 250ms;
}

.columns {
  display: flex;
  flex-direction: row;
}
.columns > * {
  flex: 1;
}

.intent > h1 {
  margin-top: 0;
  padding: 0;
  font-weight: normal;
  display: flex;
  align-items: center;
}

.version {
  margin-left: 2em;
}
.ratio {
  vertical-align: super;
  font-size: smaller;
  color: gray;
}

perpetuo-deployment-state {
  font-size: 32px;
}

.action {
  margin-top: 1em;
}
.action span {
  color: #555;
}

.operations {
  padding: 24px 16px;
}

.operations {
  border: solid 1px;
  border-left: solid 24px;
}
.operations[data-status=succeeded] {
  border-color: #0f9d58;
}
.operations[data-status=failed],
.operations[data-status=flopped] {
  border-color: var(--perpetuo-red);
}
.operations[data-status=inProgress] {
  border-color: var(--perpetuo-blue);
}
:host([state=reverted]) .operations {
  border-color: #555;
}

perpetuo-deployment-operation:not(:first-of-type) {
  margin-top: 16px;
}

.operations h1 {
  margin: 0;
  font-weight: normal;
}

paper-card {
  margin-bottom: 1em;
}
paper-card h1, paper-card h2 {
  margin: 0;
  font-weight: normal;
}

paper-card.intent {
  margin-right: 8px;
  padding: 8px 16px;
}

paper-card.actions {
  margin-left: 8px;
  padding: 8px 16px;
}

.cancel {
  background-color: var(--perpetuo-red);
  color: #fff;
}
.revert {
  background: #26a69a;
  color: #fff;
}

#plan {
  margin: 24px 0;
  border-collapse: collapse;
  table-layout: fixed;
}
#plan th {
  padding: 8px 32px;
  text-align: center;
  font-weight: 100;
  color: hsl(0,0%,87%);
}
#plan th[data-active] {
  color: hsl(0,0%,38%);
  font-weight: 500;
}
:host(:not([state=reverted])) #plan th:not([data-status=future]) {
  color: hsl(0,0%,38%);
}
:host([state=reverted]) #plan th {
  color: #555;
}
#plan td {
  position: relative;
  text-align: center;
  padding: 0;
}
#plan td .round {
  position: relative;
  display: inline-flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  border-radius: 50%;
  border-left: solid 5px #fff;
  border-right: solid 5px #fff;
  min-height: 32px;
  min-width: 32px;
  font-size: 16px;

  background-color: hsl(0,0%,57%);
  color: #fff;
}
#plan td .round[data-status=succeeded] {
  background-color: #0f9d58;
}
#plan td .round[data-status=failed],
#plan td .round[data-status=flopped] {
  background-color: var(--perpetuo-red);
}
#plan td .round[data-status=inProgress] {
  background-color: var(--perpetuo-blue);
}
:host([state=reverted]) #plan td .round {
  background-color: #555;
}
#plan .bar {
  border: solid 1px #bdbdbd;
  margin: 16px 0;
}
#plan td:first-of-type .bar {
  width: 50%;
  align-self: flex-end;
}
#plan td:last-of-type .bar {
  width: 50%;
  align-self: flex-start;
}
#plan td:last-of-type:first-of-type .bar {
  visibility: hidden;
}

paper-progress {
  width: auto;
  --paper-progress-active-color: var(--perpetuo-blue);
  --paper-progress-container-color: #fff;
}
</style>
<perpetuo-app-toolbar>
  <div slot="title" style="display: flex; align-items: center;">
    <a href="/deployment-requests"><paper-icon-button icon="arrow-back" style="color:#fff"></paper-icon-button></a>
    <span style="margin-left: 20px; font-size: var(--paper-font-headline_-_font-size);">Deployment Request #[[deploymentRequestId]]</span>
  </div>
</perpetuo-app-toolbar>
<paper-progress indeterminate hidden$="[[!noData]]"></paper-progress>

<div class="body" hidden$="[[noData]]">
  <div class="columns">
    <paper-card class="intent">
      <h1>
        <span class="product-name">[[data.productName]]</span>
        <span class="version">
          <template is="dom-repeat" items="[[version]]">
            <template is="dom-if" if="[[index]]">+</template>
            [[item.value]]<template is="dom-if" if="[[item.ratio]]">&nbsp;<span class="ratio">[[asPercentage(item.ratio)]]%</span></template>
          </template>
        </span>
      </h1>
      <table id="plan">
        <tr>
          <template is="dom-repeat" items="[[steps]]">
            <th data-active$="[[item.active]]" data-status$="[[item.status]]">[[item.label]]</th>
          </template>
        </tr>
        <tr>
          <template is="dom-repeat" items="[[steps]]">
            <td>
              <div style="display: flex; flex-direction: column;">
                <div class="bar"></div>
              </div>
              <div style="position: absolute; top: 0; left: 0; width: 100%">
                <div class="round" data-status$="[[item.status]]">[[inc(index)]]</div>
              </div>
            </td>
          </template>
        </tr>
      </table>
      <div>
        <p>Requested by [[data.creator]] on [[timestampToDates(data.creationDate)]]</p>
        <template is="dom-if" if="[[data.comment]]"><p><marked-element markdown="[[data.comment]]"></marked-element></p></template>
      </div>
    </paper-card>
    <paper-card class="actions">
      <perpetuo-deployment-state state="[[state]]"></perpetuo-deployment-state>
      <div hidden$="[[!data.outdatedBy]]">
        This request is outdated by
        <a href="/deployment-requests/[[data.outdatedBy]]">another request</a>
      </div>
      <div>
        <template is="dom-repeat" items="[[data.actions]]">
          <div class="action">
            <paper-button disabled="[[computeDisabled(isInAction, item.rejected)]]" on-tap="onActionTap" raised>[[humanReadable(item.type)]]</paper-button>
            <span>[[item.rejected]]</span>
          </div>
        </template>
      </div>
    </paper-card>
  </div>
  <template is="dom-repeat" items="[[operationGroups]]" as="operationGroup">
    <paper-card class="operations" data-status$="[[operationGroup.status]]">
      <h1>[[operationGroup.label]]</h1>
      <template is="dom-repeat" items="[[operationGroup.operations]]">
        <perpetuo-deployment-operation index="[[sub(operationGroup.operations.length, index)]]" data="[[item]]" opened="[[isOperationOpened(item.id)]]" on-opened-changed="onOperationOpenedChanged"></perpetuo-deployment-operation>
      </template>
    </paper-card>
  </template>
</div>

<perpetuo-revert-plan-dialog id="revertPlanDialog"></perpetuo-revert-plan-dialog>
<perpetuo-conflicting-deployment-requests id="conflictPanel"></perpetuo-conflicting-deployment-requests>
`;
  }

  static get is() { return 'perpetuo-deployment-request'; }

  static get properties() {
    return {
      deploymentRequestId: { type: String, observer: 'onDeploymentRequestIdChanged' },
      noData: { type: Boolean, value: true },
      active: { type: Boolean, observer: 'onActiveChanged' },
      data: Object,
      state: { type: String, reflectToAttribute: true, computed: 'computeState(data)' },
      operationGroups: Array,
      version: { type: Array, computed: 'computeVersion(data)' },
      isInAction: Boolean,
      revertPlan: Object,
      defaultVersion: String,
    }
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

  constructor() {
    super();

    this.client = new Perpetuo.Client();
    this.refresher = new Perpetuo.AsyncRepeater(() => this.refresh(), 5000);
  }

  onDeploymentRequestIdChanged() {
    // TODO: have a dedicated view-model instead:
    this.noData = true;
    this.data = {
      creationDate: '',
      state: '',
      operations: []
    };
    this.operationGroups = [];
    this.operationIdToOpened = new Map(); // A Map is required to distinguish inexistence and closed-by-user
    this.refresh();
  }

  onActiveChanged(active) {
    this.refresher.suspended = !active;
  }

  onActionTap(e) {
    switch (e.model.item.type) {
      case 'deploy':
        return this.onDeployTap(e);
      case 'revert':
        return this.onRevertTap(e);
      case 'stop':
        return this.onStopTap(e);
      case 'abandon':
        return this.onAbandonTap(e);
    }
  }

  actionLock(f) {
    if (!this.isInAction) {
      this.isInAction = true;
      const releaseLock = () => {
        this.isInAction = false;
      };
      f().then(releaseLock)
         .catch(releaseLock);
    }
  }

  onDeployTap(e) {
    this.actionLock(() =>
      this.client.stepDeploymentRequest(this.deploymentRequestId, this.data.operations.length)
          .then(_ => this.refresh())
          .catch(e => {
            if (e.detail && e.detail.conflicts) {
              const panel = this.$.conflictPanel;
              panel.conflicts = e.detail.conflicts;
              panel.open();
            }
          })
    );
  }

  showRevertPlan(revertPlan) {
    return new Promise((resolve, reject) => {
      const dialog = this.$.revertPlanDialog;
      const onOpenedChanged = (e) => {
        if (!e.detail.value) {
          dialog.removeEventListener('opened-changed', onOpenedChanged);
          if (dialog.closingReason.confirmed) {
            resolve(dialog.defaultVersion);
          } else {
            reject();
          }
        }
      };
      dialog.addEventListener('opened-changed', onOpenedChanged);
      dialog.open(revertPlan);
    });
  }

  onRevertTap(e) {
    this.actionLock(() => {
      const deploymentRequestId = this.deploymentRequestId
      const operationCount = this.data.operations.length
      return this.client.deviseRevertPlan(deploymentRequestId)
                 .then(r => r.json())
                 .then(revertPlan => this.showRevertPlan(revertPlan))
                 .then(defaultVersion => // possibly undefined
                   this.client.revertDeploymentRequest(deploymentRequestId, operationCount, defaultVersion)
                 )
                 .catch(_ => this.refresh())
    });
  }

  onStopTap(e) {
    this.actionLock(() =>
      this.client.stopDeploymentRequest(this.deploymentRequestId, this.data.operations.length)
    );
  }

  onAbandonTap(e) {
    this.actionLock(() =>
      this.client.abandonDeploymentRequest(this.deploymentRequestId)
    );
  }

  computeDisabled(isInAction, rejected) {
    return isInAction || rejected;
  }

  computeState(data) {
    return data.state;
  }

  computeVersion(data) {
    return Array.isArray(data.version) ? data.version : [{ value: data.version }];
  }

  asPercentage(ratio) {
    return ratio * 100;
  }

  humanReadable(actionName) {
    return actionName.replace(/([A-Z])/g, ' $1');
  }

  refresh() {
    return this.deploymentRequestId === undefined ?
           Promise.resolve(false)
      : this.client.fetchDeploymentRequest(this.deploymentRequestId).then(data => {
        if (data.id == this.deploymentRequestId) { // Support out-of-order responses
          this.data = data;
          const maxOperationId = this.data.operations.reduce((max, op) => max < op.id ? op.id : max, -1);
          if (!this.operationIdToOpened.has(maxOperationId)) {
            this.operationIdToOpened.set(maxOperationId, true);
          }

          this.operationGroups =
          data.operations.reduce((operationGroups, op) => {
            let lastOperationGroup = 0 < operationGroups.length ? operationGroups[operationGroups.length - 1] : null;
            if (!lastOperationGroup ||
                !(op.kind === lastOperationGroup[0].kind &&
                  op.planStepIds.length === lastOperationGroup[0].planStepIds.length && op.planStepIds.every(id => lastOperationGroup[0].planStepIds.includes(id)))) {
                    lastOperationGroup = [];
                    operationGroups.push(lastOperationGroup);
            }
            lastOperationGroup.push(op);
            return operationGroups;
          }, [])
              .map(operations => ({
                operations: operations,
                status: operations[0].status,
                label: computeOperationGroupLabel(operations[0].planStepIds, data.plan)
              }));

          const planStepsAndLastOperations =
          data.plan.map(planStep => [
            planStep,
            data.operations.filter(op => op.planStepIds.includes(planStep.id))[0]
          ]);
          this.steps =
          planStepsAndLastOperations.map((planStepAndLastOperations, i) => {
            const planStep = planStepAndLastOperations[0];
            const lastOperation = planStepAndLastOperations[1];
            return {
              label: computePlanStepLabel(planStep),
              status: lastOperation ? lastOperation.status : 'future',
              active: (
                !(data.state === 'reverted') &&
                (lastOperation ? lastOperation.status !== 'succeeded' : (i === 0 || (planStepsAndLastOperations[i - 1][1] && planStepsAndLastOperations[i - 1][1].status === 'succeeded')))
              )
            }
          });

          this.noData = false;
        }
      });
  }

  isOperationOpened(id) {
    return this.operationIdToOpened.get(id);
  }
  onOperationOpenedChanged(e) {
    this.operationIdToOpened.set(e.model.item.id, e.detail.value);
  }

  sub(a, b) {
    return a - b;
  }

  inc(i) {
    return i + 1;
  }
}

customElements.define(PerpetuoDeploymentRequest.is, PerpetuoDeploymentRequest);
