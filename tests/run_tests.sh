pytest test__osm.py --html=outputs/test__osm.html --self-contained-html --log-file=outputs/test__osm.log

pytest test__gtfs.py --html=outputs/test__gtfs.html --self-contained-html --log-file=outputs/test__gtfs.log

pytest test__ovm.py --html=outputs/test__ovm.html --self-contained-html --log-file=outputs/test__ovm.log

pytest test__worldpop.py --html=outputs/test__worldpop.html --self-contained-html --log-file=outputs/test__worldpop.log

pytest test__geobound.py --html=outputs/test__geobound.html --self-contained-html --log-file=outputs/test__geobound.log
