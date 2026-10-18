data "aws_iam_policy_document" "retail_s3_readonly" {

  statement {
    sid    = "ListRetailBucket"
    effect = "Allow"

    actions = [
      "s3:ListBucket"
    ]

    resources = [
      data.aws_s3_bucket.existing_retail_bucket.arn
    ]
  }

  statement {
    sid    = "ReadRetailObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject"
    ]

    resources = [
      "${data.aws_s3_bucket.existing_retail_bucket.arn}/*"
    ]
  }
}

output "retail_s3_policy" {
  value = data.aws_iam_policy_document.retail_s3_readonly.json
}