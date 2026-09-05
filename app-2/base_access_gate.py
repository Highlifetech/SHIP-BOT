"""Fail-closed shipping access while delegated Base authorization is unavailable.

An identity cookie or shared dashboard token is NOT a record permission grant.
Do not remove this gate until record, field, attachment and export permissions
are verified server-side with the acting user's Lark authorization. Legacy
shipping sheets cannot be treated as an authorized fallback for Base records.
"""
from flask import jsonify, request


def register(app):
    @app.before_request
    def require_verified_base_access():
        path = request.path
        if path == '/dashboard/health':
            return jsonify(ok=True, access='locked'), 200, {'Cache-Control': 'no-store'}
        protected = (path == '/dashboard' or path.startswith('/dashboard/')
                     or path == '/fulfillment' or path.startswith('/fulfillment/')
                     or path == '/packing-list' or path.startswith('/packing-list/')
                     or path == '/api' or path.startswith('/api/'))
        if not protected:
            return None
        # There is intentionally no environment flag, shared-token exception,
        # owner-based approximation, or app-token fallback that bypasses this.
        response = jsonify(
            error='Shipping access is temporarily locked. Your existing Lark Base '
                  'Advanced Permissions cannot yet be verified by this app. '
                  'Use Lark Base directly until delegated authorization is connected.',
            code='BASE_PERMISSION_VERIFICATION_UNAVAILABLE')
        response.status_code = 403
        response.headers['Cache-Control'] = 'no-store, private'
        response.headers['Vary'] = 'Cookie, Authorization'
        return response
