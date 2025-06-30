# def test_config(request):  # request is a pytest fixture it holds meta data of pytest test methods
#     print("request", request)
#     print("request.node", request.node)  # test name
#     print("request.node.fspath", request.node.fspath)  # test file where this test method is available
#     print("request.node.fspath.dirname", request.node.fspath.dirname)

def test_count(read_config):
    config_file_path = read_config
    print("config file path", config_file_path)



