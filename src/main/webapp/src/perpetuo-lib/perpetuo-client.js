export class Client {
  constructor(host) {
    this.host = host;

    this.hasMoreDeploymentRequests = undefined; // Unknown until first request
  }

  redirectToAuthorizeURL(win) {
    return fetch('/api/auth/identity', {
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json' },
      credentials: 'include'
    }).then(r => {
      if (!r.headers.get('X-Perpetuo-JWT')) {
        const url = r.headers.get('X-Perpetuo-Login')
        win.location.replace(`${url}&redirect_uri=${window.location.origin}/identify&state=${window.location.href}`);
      }
    });
  }

  identify(code) {
    return Client
      .fetch(`/api/auth/jwt?code=${code}&redirect_uri=${window.location.origin}/identify`)
      .then(r => {
        window.localStorage.jwt = r.headers.get('X-Perpetuo-JWT');
      });
  }

  signOut() {
    window.localStorage.removeItem('jwt');
  }

  fetchIdentity() {
    return Client.fetch('/api/auth/identity').then(r => r.json()).then(o => o.name).catch(() => null);
  }

  fetchDeploymentRequest(id) {
    return Client.fetch(`/api/unstable/deployment-requests/${id}`)
      .then(_ => _.json());
  }

  fetchDeploymentRequests(options) {
    options = Object.assign({ where: [], limit: 20, offset: 0 }, options);
    options.limit = options.limit + 1; // Ask for one more element to know if more are available TODO: remove
    return Client.fetch('/api/unstable/deployment-requests', { method: 'POST', body: JSON.stringify(options) })
      .then(_ => _.json()).then(deploymentRequests => {
        this.hasMoreDeploymentRequests = deploymentRequests.length === options.limit; // TODO: handle in caller
        if (this.hasMoreDeploymentRequests) {
          deploymentRequests.pop();
        }
        return deploymentRequests;
      });
  }

  createDeploymentRequest(productName, version, plan, comment) {
    return Client.fetch('/api/deployment-requests', {
      method: 'POST',
      body: JSON.stringify({
        productName: productName,
        version: version,
        plan: plan,
        comment: comment
      })
    });
  }
  stepDeploymentRequest(deploymentRequestId, operationCount) {
    return this.actOnDeploymentRequest(deploymentRequestId, 'step', { operationCount: operationCount });
  }

  deviseRevertPlan(deploymentRequestId) {
    return this.actOnDeploymentRequest(deploymentRequestId, 'devise-revert-plan');
  }

  revertDeploymentRequest(deploymentRequestId, operationCount, defaultVersion) {
    return this.actOnDeploymentRequest(deploymentRequestId, 'revert', { operationCount: operationCount, defaultVersion: defaultVersion });
  }

  stopDeploymentRequest(deploymentRequestId, operationCount) {
    return this.actOnDeploymentRequest(deploymentRequestId, 'stop', { operationCount: operationCount });
  }

  abandonDeploymentRequest(deploymentRequestId) {
    return this.actOnDeploymentRequest(deploymentRequestId, 'abandon');
  }

  actOnDeploymentRequest(deploymentRequestId, actionName, body) {
    // note: below, JSON.stringify removes the undefined values
    return Client.fetch(`/api/deployment-requests/${deploymentRequestId}/actions/${actionName}`, { method: 'POST', body: JSON.stringify(body) });
  }

  fetchProducts() {
    return Client.fetch('/api/products').then(_ => _.json()).then(products => products.map(_ => {
      return { name: _.name, active: _.active }
    }));
  }

  getAllowedActions() {
    return Client.fetch('/api/actions').then(_ => _.json());
  }

  static fetch(url, options) {
    return fetch(url, Object.assign({
      headers: { 'Accept': 'application/json', 'Content-Type': 'application/json', 'Authorization': window.localStorage.jwt },
      credentials: 'include'
    }, options || {})).then(r =>
      r.ok ? r : r.json().then(o => Promise.reject({ status: r.status, detail: o }))
    );
  }

  updateProduct(name, active) {
    return Client.fetch('/api/products', {
      method: 'PUT',
      body: JSON.stringify({ name: name, active: active })
    });
  }
}
