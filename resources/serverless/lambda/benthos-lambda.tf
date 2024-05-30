resource "aws_lambda_function" "bento-lambda" {
  function_name = "bento-lambda"
  role          = "${aws_iam_role.lambda-role.arn}"
  handler       = "bento-lambda"
  runtime       = "go1.x"

  s3_bucket = "${var.bucket_name}"
  s3_key    = "bento-lambda-${var.version}.zip"

  environment {
    variables = {
      LAMBDA_ENV = "${data.template_file.conf.rendered}"
    }
  }
}