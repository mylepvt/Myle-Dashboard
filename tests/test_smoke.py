"""Automated smoke tests — auth, CSRF, role gates, security headers."""

from __future__ import annotations

import os

ADMIN_USER = 'admin'
ADMIN_PASSWORD = os.environ['BOOTSTRAP_ADMIN_PASSWORD']


def _login(client, username: str, password: str, follow_redirects: bool = True):
    client.get('/login')
    with client.session_transaction() as sess:
        token = sess.get('_csrf_token', '')
    return client.post(
        '/login',
        data={'username': username, 'password': password, 'csrf_token': token},
        follow_redirects=follow_redirects,
    )


def test_login_page_ok(client):
    r = client.get('/login')
    assert r.status_code == 200


def test_security_headers_on_public_page(client):
    r = client.get('/login')
    assert r.headers.get('X-Content-Type-Options') == 'nosniff'
    assert r.headers.get('X-Frame-Options') == 'SAMEORIGIN'


def test_csrf_blocks_post_without_token(client):
    """State-changing POST without csrf_token must fail before business logic."""
    r = client.post(
        '/leads/add',
        data={'name': 'x', 'phone': '9999999999'},
        headers={'X-Requested-With': 'XMLHttpRequest'},
    )
    assert r.status_code == 403


def test_unauthenticated_admin_pipeline_redirects_to_login(client):
    r = client.get('/admin/pipeline-analytics', follow_redirects=False)
    assert r.status_code in (302, 303, 307, 308)
    assert '/login' in (r.headers.get('Location') or '')


def test_admin_can_load_pipeline_analytics(admin_client):
    r = admin_client.get('/admin/pipeline-analytics')
    assert r.status_code == 200
    assert b'pipeline' in r.data.lower() or b'analytics' in r.data.lower()


def test_admin_can_load_leader_coaching(admin_client):
    r = admin_client.get('/leader/coaching')
    assert r.status_code == 200


def test_team_cannot_access_admin_pipeline(team_client):
    r = team_client.get('/admin/pipeline-analytics', follow_redirects=False)
    assert r.status_code in (302, 303, 307, 308)
    loc = r.headers.get('Location') or ''
    assert '/dashboard' in loc


def test_login_success_redirects_admin(client):
    r = _login(client, ADMIN_USER, ADMIN_PASSWORD, follow_redirects=False)
    assert r.status_code in (302, 303, 307, 308)
    assert '/admin' in (r.headers.get('Location') or '')
