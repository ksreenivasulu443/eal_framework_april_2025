# def test_config(request):  # request is a pytest fixture it holds meta data of pytest test methods
#     print("request", request)
#     print("request.node", request.node)  # test name
#     print("request.node.fspath", request.node.fspath)  # test file where this test method is available
#     print("request.node.fspath.dirname", request.node.fspath.dirname)

def test_count(read_config):
    config_data = read_config
    print("config data is", config_data, type(config_data), end='\n')
    print("source config is", config_data['source'],end='\n')
    print("target config is", config_data['target'],end='\n')
    print("validation config is", config_data['validations'],end='\n')





