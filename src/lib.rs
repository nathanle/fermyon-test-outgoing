use futures::{future, Future, SinkExt, StreamExt};
use spin_sdk::{
    http::{
        self, Headers, IncomingRequest, IncomingResponse, Method, OutgoingBody, OutgoingRequest,
        OutgoingResponse, ResponseOutparam, Scheme,
    },
    http_component,
};
use url::Url;
mod s3;

#[http_component]
async fn handle_request(request: IncomingRequest, response_out: ResponseOutparam) {
    //async fn handle_request(request: IncomingRequest) {
    let headers = &request.headers().entries();
    //let borrowed_headers = &request.headers().entries();
    let borrowed_method = &request.method();
    //let owned_response_out = response_out;
    /*
    let url_vec = vec![
        "https://us-east-1.linodeobjects.com",
        "https://us-southeast-1.linodeobjects.com",
    ];
    let region_vec = vec!["us-east-1", "us-southeast-1"];
    */
    //for (url_i, region_item) in url_vec.iter().zip(region_vec.iter()) {
    let url_i = "https://us-us-east-1.linodeobjects.com";
    let region_item = "us-east-1";
    match borrowed_method {
        Method::Put => {
            println!("{:#?}", headers.iter());
            let Some(url) = headers.iter().find_map(|(k, v)| {
                (k == "spin-full-url")
                    .then_some(v)
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|v| Url::parse(v).ok())
                    .and_then(|v| s3::url_parse(v))
                //.and_then(|v| Url::parse(v).ok())
            }) else {
                bad_request(response_out);
                return;
            };

            //let uri_components = s3::url_parse(url);
            let path_filename = format!("{}{}", url.re_path, url.re_filename);
            let url_item = format!("{}{}", url_i, path_filename);
            let signed_url = s3::sign(
                url_item.to_string(),
                &region_item,
                "example-bucket-natle",
                String::from("PUT"),
            );
            println!("{}", signed_url);

            match replicate_to_obj_endpoint(request, signed_url).await {
                Ok((request_copy, incoming_response)) => {
                    let mut incoming_response_body = incoming_response.take_body_stream();
                    let outgoing_response = OutgoingResponse::new(
                        Headers::from_list(
                            &headers
                                .clone()
                                .into_iter()
                                .filter(|(k, _)| k == "content-type")
                                .collect::<Vec<_>>(),
                        )
                        .unwrap(),
                    );

                    let mut outgoing_response_body = outgoing_response.take_body();

                    response_out.set(outgoing_response);

                    let response_copy = async move {
                        while let Some(chunk) = incoming_response_body.next().await {
                            outgoing_response_body.send(chunk?).await?;
                        }
                        Ok::<_, anyhow::Error>(())
                    };

                    let (request_copy, response_copy) =
                        future::join(request_copy, response_copy).await;

                    if let Err(e) = request_copy.and(response_copy) {
                        eprintln!("error piping to and from {:?}: {e}", url);
                    }
                }
                Err(e) => {
                    eprintln!("Error sending outgoing request to {:?}: {e}", url);
                    server_error(response_out);
                }
            }
        }
        _ => method_not_allowed(response_out),
    }
    //}
}

async fn replicate_to_obj_endpoint(
    incoming_request: IncomingRequest,
    url: Url,
) -> anyhow::Result<(impl Future<Output = anyhow::Result<()>>, IncomingResponse)> {
    let outgoing_request = OutgoingRequest::new(Headers::new());
    outgoing_request.set_method(&Method::Put).unwrap();
    outgoing_request
        .set_path_with_query(Some(url.path()))
        .map_err(|()| anyhow::anyhow!("unable to set path"))?;
    outgoing_request
        .set_scheme(Some(&match url.scheme() {
            "http" => Scheme::Http,
            "https" => Scheme::Https,
            scheme => Scheme::Other(scheme.into()),
        }))
        .map_err(|()| anyhow::anyhow!("unable to set scheme"))?;
    outgoing_request
        .set_authority(Some(url.authority()))
        .map_err(|()| anyhow::anyhow!("unable to set authority"))?;

    let mut body = outgoing_request.take_body();

    let response = http::send::<_, IncomingResponse>(outgoing_request).await?;

    let mut stream = incoming_request.into_body_stream();

    let copy = async move {
        while let Some(chunk) = stream.next().await {
            body.send(chunk?).await?;
        }
        Ok::<_, anyhow::Error>(())
    };

    Ok((copy, response))
}

fn server_error(response_out: ResponseOutparam) {
    respond(500, response_out)
}

fn bad_request(response_out: ResponseOutparam) {
    respond(400, response_out)
}

fn method_not_allowed(response_out: ResponseOutparam) {
    respond(405, response_out)
}

fn respond(status: u16, response_out: ResponseOutparam) {
    let response = OutgoingResponse::new(Headers::new());
    response.set_status_code(status).unwrap();

    let body = response.body().expect("response should be writable");

    response_out.set(response);

    OutgoingBody::finish(body, None).expect("OutgoingBody::finish should succeed");
}
