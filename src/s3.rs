//Written by Nathan LeSueur @ Akamai
use regex::Regex;
use rusty_s3::{Bucket, Credentials, S3Action, UrlStyle};
use std::env;
use std::time::Duration;
use url::Url;

//let key = env::var("AKAMAI_ACCESS_KEY_ID").expect("AKAMAI_ACCESS_KEY_ID not set");
const KEY: &'static str = env!(
    "AKAMAI_ACCESS_KEY_ID",
    "AKAMAI_ACCESS_KEY_ID not set at compile time"
);
//let secret = env::var("AKAMAI_SECRET_ACCESS_KEY").expect("AKAMAI_SECRET_ACCESS_KEY not set");
const SECRET: &'static str = env!(
    "AKAMAI_SECRET_ACCESS_KEY",
    "AKAMAI_SECRET_ACCESS_KEY not set at compile time"
);

pub struct UrlComponents {
    pub re_filename: String,
    pub re_path: String,
}

pub fn get_url_regex(pattern: &str) -> Regex {
    match Regex::new(pattern) {
        Ok(regex) => regex,
        Err(e) => {
            panic!("Error: {}", e);
        }
    }
}

pub fn get_url_parse(url_string: &str) -> Url {
    match Url::parse(url_string) {
        Ok(u) => u,
        Err(e) => {
            panic!("Error: {}", e);
        }
    }
}

pub fn get_captures(re: Regex, data: &str) -> regex::Captures<'_> {
    match re.captures(data) {
        Some(caps) => caps,
        None => {
            panic!("Error: Cannot capture regex groups expected in URL pattern!");
        }
    }
}

pub fn url_parse(url: Url) -> UrlComponents {
    let re = get_url_regex(r"^([a-z][a-z0-9+\-.]*://([^/?#]+)?)?([a-z0-9\-._~%!$&'()*+,;=:@/]*)");
    let path_re = get_url_regex(r"^([/\S]*[/]{1})(.*)");
    let u = url.to_owned().to_string();
    let cap = get_captures(re.clone(), &u);
    let path_cap = get_captures(path_re.clone(), cap.get(3).map_or("", |m| m.as_str()));
    UrlComponents {
        re_filename: String::from(path_cap.get(2).map_or("", |m| m.as_str())),
        re_path: String::from(path_cap.get(1).map_or("", |m| m.as_str())),
    }
}

pub fn get_signed_url(
    url_string: String,
    filename: String,
    bucket_name: String,
    region: String,
    credentials: &Credentials,
) -> Url {
    println!("Creds: {:?}", &credentials);
    println!("Region: {:?}", &region);
    println!("Bucket name: {:?}", &bucket_name);
    println!("Filename: {:?}", &filename);
    println!("URL String: {:?}", &url_string);
    let endpoint: Url = url_string.parse().expect("Invalid endpoint URL");
    let bucket = Bucket::new(
        endpoint,
        UrlStyle::VirtualHost,
        bucket_name.to_owned(),
        region.to_owned(),
    )
    .expect("Invalid bucket configuration");

    let presigned_url_duration = Duration::from_secs(3600); // URL valid for 1 hour
    let action = bucket.get_object(Some(&credentials), &filename);
    let signed_url = action.sign(presigned_url_duration);

    signed_url
}

pub fn put_signed_url(
    url_string: String,
    filename: String,
    bucket_name: String,
    region: String,
    credentials: &Credentials,
) -> Url {
    println!("Creds: {:?}", &credentials);
    println!("Region: {:?}", &region);
    println!("Bucket name: {:?}", &bucket_name);
    println!("Filename: {:?}", &filename);
    println!("URL String: {:?}", &url_string);
    let endpoint: Url = url_string.parse().expect("Invalid endpoint URL");
    let bucket = Bucket::new(
        endpoint,
        UrlStyle::VirtualHost,
        bucket_name.to_owned(),
        region.to_owned(),
    )
    .expect("Invalid bucket configuration");

    let presigned_url_duration = Duration::from_secs(3600); // URL valid for 1 hour
    let action = bucket.put_object(Some(&credentials), &filename);
    let signed_url = action.sign(presigned_url_duration);
    println!("{}", signed_url);

    signed_url
}

pub fn delete_signed_url(
    url_string: String,
    filename: String,
    bucket_name: String,
    region: String,
    credentials: &Credentials,
) -> Url {
    println!("Creds: {:?}", &credentials);
    println!("Region: {:?}", &region);
    println!("Bucket name: {:?}", &bucket_name);
    println!("Filename: {:?}", &filename);
    println!("URL String: {:?}", &url_string);
    let endpoint: Url = url_string.parse().expect("Invalid endpoint URL");
    let bucket = Bucket::new(
        endpoint,
        UrlStyle::VirtualHost,
        bucket_name.to_owned(),
        region.to_owned(),
    )
    .expect("Invalid bucket configuration");

    let presigned_url_duration = Duration::from_secs(3600); // URL valid for 1 hour
    let action = bucket.delete_object(Some(&credentials), &filename);
    let signed_url = action.sign(presigned_url_duration);
    println!("{}", signed_url);

    signed_url
}

pub fn sign(assem_url: String, region: &str, bucket_name: &str, method: String) -> Url {
    let credentials = Credentials::new(KEY, SECRET);
    let og_url = get_url_parse(&assem_url);
    println!("URL Components: {:?}", og_url);
    let url_components = url_parse(og_url);

    let signed_url: Url = if method == "GET" {
        let s = get_signed_url(
            assem_url,
            url_components.re_filename,
            bucket_name.to_string(),
            region.to_string(),
            &credentials,
        );
        s
    } else if method == "PUT" {
        let s = put_signed_url(
            assem_url,
            url_components.re_filename,
            bucket_name.to_string(),
            region.to_string(),
            &credentials,
        );
        s
    } else if method == "DELETE" {
        let s = delete_signed_url(
            assem_url,
            url_components.re_filename,
            bucket_name.to_string(),
            region.to_string(),
            &credentials,
        );
        s
    } else {
        let s = get_signed_url(
            assem_url,
            url_components.re_filename,
            bucket_name.to_string(),
            region.to_string(),
            &credentials,
        );

        s
    };

    signed_url
}
