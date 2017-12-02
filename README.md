# slidestrawberry 简介

## Getting Started(如何部署启动)
* 进入安装目录
```
cd <directory containing this file>
```

* 安装依赖包
```
$VENV/bin/pip install -e .
```

* 安装开发环境
```
$VENV/bin/python setup.py develop
```

* 初始化数据库
```
$VENV/bin/initialize_db development.ini
```

* 启动服务 
```
$VENV/bin/pserve development.ini
```

* 启动后台服务
```
$VENV/bin/celery worker -A pyramid_celery.celery_app --ini development.ini
$VENV/bin/celery beat -A pyramid_celery.celery_app --ini development.ini
```
