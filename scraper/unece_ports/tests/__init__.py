import os

from scrapy.http import HtmlResponse, Request


def fake_response_from_file(file_name, url=None):
    """
    Create a Scrapy fake HTTP response from a HTML file
    @param file_name: The relative filename from the responses directory,
                      but absolute paths are also accepted.
    @param url: The URL of the response.
    returns: A scrapy HTTP response which can be used for unittesting.
    """
    if not url:
        url = 'https://service.unece.org/trade/locode/gt.htm'
    request = Request(url=url)
    if not file_name[0] == '/':
        responses_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(responses_dir, file_name)
    else:
        file_path = file_name
    with open(file_path, 'r') as f:
        file_content = f.read()
    response = HtmlResponse(
        url=url,
        request=request,
        body=file_content,
        encoding='utf-8'
    )
    return response
