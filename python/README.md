# examples-Python/python
The **python** subdirectory of this repository contains examples to help you get started working with the data and tools hosted by the ISB-CGC on the Google Cloud Platform.

### Programmatic API Examples
The ISB-CGC programmatic API is implemented using Google Cloud Endpoints.  These services run on App Engine and can be accessed in several different ways.  You can use the [APIs Explorer](https://apis-explorer.appspot.com/apis-explorer/?base=https://api-dot-isb-cgc.appspot.com/_ah/api#p/) to try them out directly in your web browser.  From Python, you can construct the https requests programmatically as shown in **api_test_url.py** or you can build a service object and use them as shown in **api_test_service.py**.  We have found that accessing these endpoints using a service object is roughly twice as fast as the https requests approach, so we suggest that you start there.

