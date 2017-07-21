#!/usr/bin/env python
# coding=utf-8

#
# Copyright (c) 2015-2018  Terry Xi
# All Rights Reserved.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
# TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#


from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import configure_mappers
import zope.sqlalchemy
import happybase

# import or define all models here to ensure they are attached to the
# Base.metadata prior to any initialization routines
from .meta import Base as Base

# run configure_mappers after defining all of the models to ensure
# all relationships can be setup
configure_mappers()


def _get_pointed_value(settings, prefix):
    for k, v in settings.items():
        if prefix in k:
            return v.strip()
    return ''


def _get_mod_tables(mod):
    _n = []
    for d in dir(mod):
        n = getattr(mod, d)
        if n and hasattr(n, '__tablename__'):
            t = Table(getattr(n, '__tablename__'), n, [c for c in dir(n) if not c.startswith('_') and c != 'id'])
            if t not in _n:
                _n.append(t)
    return _n


def _parse_create_tables(engine, config):
    if engine.name == 'hbase':
        mod = __import__(config, globals(), locals(), [config.split('.')[-1]])
        mod_instances = _get_mod_tables(mod)
        engine.engine.open()
        _tables = engine.engine.tables()
        for m in mod_instances:
            if m.name not in _tables:
                family = {}
                for c in m.columns:
                    if c not in family:
                        family[c] = {}
                engine.engine.create_table(m.name, family)
    else:
        Base.metadata.create_all(engine.engine)


class Engine(object):
    def __init__(self, engine, name=''):
        self.engine = engine
        self.name = name


class EngineFactory(object):
    def __init__(self, factory, name=''):
        self.factory = factory
        self.name = name


class Table(object):
    def __init__(self, name, inst, columns=None):
        self.name = name
        self.inst = inst
        self.columns = columns


def get_engine(settings, prefix='sql.'):
    value = _get_pointed_value(settings, prefix)
    if value.startswith('hbase:'):
        import urlparse
        value = urlparse.urlparse(value)
        host, port = value.netloc.split(':')
        return Engine(happybase.Connection(host=host, port=int(port)), 'hbase')
    else:
        return Engine(engine_from_config(settings, prefix), 'sqlalchemy')


def create_tables(engine, settings, prefix='model.'):
    value = _get_pointed_value(settings, prefix)
    if not value:
        return
    _parse_create_tables(engine, value)


def get_session_factory(engine):
    if engine.name == 'hbase':
        return EngineFactory(engine.engine, engine.name)
    else:
        factory = sessionmaker()
        factory.configure(bind=engine)
        return EngineFactory(factory, engine.name)


def get_tm_session(session_factory, transaction_manager):
    """
    Get a ``sqlalchemy.orm.Session`` instance backed by a transaction.

    This function will hook the session to the transaction manager which
    will take care of committing any changes.

    - When using pyramid_tm it will automatically be committed or aborted
      depending on whether an exception is raised.

    - When using scripts you should wrap the session in a manager yourself.
      For example::

          import transaction

          engine = get_engine(settings)
          session_factory = get_session_factory(engine)
          with transaction.manager:
              dbsession = get_tm_session(session_factory, transaction.manager)

    """
    if session_factory.name == 'hbase':
        session_factory.factory.open()
        return session_factory.factory
    else:
        dbsession = session_factory()
        zope.sqlalchemy.register(
            dbsession, transaction_manager=transaction_manager)
        return dbsession


def includeme(config):
    """
    Initialize the model for a Pyramid app.

    Activate this setup using ``config.include('WizDatacenter.models')``.

    """
    settings = config.get_settings()

    # use pyramid_tm to hook the transaction lifecycle to the request
    config.include('pyramid_tm')

    engine = get_engine(settings=settings)
    session_factory = get_session_factory(engine=engine)
    create_tables(engine=engine, settings=settings)
    config.registry['dbsession_factory'] = session_factory

    # make request.dbsession available for use in Pyramid
    config.add_request_method(
        # r.tm is the transaction manager used by pyramid_tm
        lambda r: get_tm_session(session_factory, r.tm),
        'dbsession',
        reify=True
    )
