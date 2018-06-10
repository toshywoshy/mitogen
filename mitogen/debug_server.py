# Copyright 2017, David Wilson
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

import functools
import gc
import os
import flask

import mitogen.debug
import mitogen.master


BASE_DIR = os.path.join(os.path.dirname(__file__), 'debug_data')
app = flask.Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, 'templates'),
    static_folder=os.path.join(BASE_DIR, 'static'),
)
app.config['TEMPLATES_AUTO_RELOAD'] = True


def call(context_id, func, *args, **kwargs):
    if context_id == mitogen.context_id:
        return func(*args, **kwargs)

    context = router.context_by_id(context_id)
    return context.call(func, *args, **kwargs)


def templated(name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            ctx = func(*args, **kwargs) or {}
            return flask.render_template(name, **ctx)
        return wrapper
    return decorator


def jsonify(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = func(*args, **kwargs) or {}
        return flask.jsonify(**ctx)
    return wrapper


@app.route('/')
@templated('index.html')
def index():
    return {
        'context_id': mitogen.context_id,
    }


@app.route('/api/contexts/<int:context_id>/routers/')
@jsonify
def routers(context_id):
    return call(context_id, mitogen.debug.get_router_info)


@app.route('/api/contexts/<int:context_id>/routers/<router_id>/streams/')
@jsonify
def streams(context_id, router_id):
    return call(context_id, mitogen.debug.get_stream_info, router_id)


r = mitogen.master.Router()
r.local(via=r.local())
app.run()
