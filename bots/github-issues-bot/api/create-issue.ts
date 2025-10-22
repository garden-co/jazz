import { VercelRequest, VercelResponse } from "@vercel/node";
import { Octokit } from "@octokit/rest";

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== "POST") {
    return res.status(405).json({ message: "Method not allowed" });
  }
  const token = process.env.GITHUB_ACCESS_TOKEN;
  const owner = process.env.GITHUB_USERNAME;
  const repo = process.env.GITHUB_REPOSITORY;
  if (!owner || !repo || !token) {
    console.error("Missing GitHub env vars");
    return res
      .status(500)
      .json({ error: "Missing GitHub environment variables" });
  }

  const { title, body } = req.body;
  if (!title || !body) {
    return res.status(400).json({ message: "Missing title or body" });
  }

  const octokit = new Octokit({ auth: process.env.GITHUB_ACCESS_TOKEN });

  try {
    const response = await octokit.rest.issues.create({
      owner,
      repo,
      title,
      body,
    });
    res.status(200).json({ url: response.data.html_url });
  } catch (err) {
    res.status(500).json({ error: err });
  }
}
