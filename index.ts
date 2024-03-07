import {existsSync, createWriteStream} from 'fs';
import fs from 'fs/promises';
import {Readable} from 'stream';
import {finished} from 'stream/promises';
import path from 'path';
import { S3 } from '@aws-sdk/client-s3';
import { exec } from 'child_process';

function convert(input: string, output: string) {
  return new Promise<void>((resolve, reject) => {
    exec(`ffmpeg -y -i ${input} -max_muxing_queue_size 4096 -threads 16 -row-mt 1 -crf 20 -qmin 1 -qmax 51 -b:v 0 -vcodec libvpx-vp9 -tile-columns 4 -auto-alt-ref 1 -lag-in-frames 25 -f webm -ss 0 -b:a 128000 -ar 48000 -acodec libopus ${output}`, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}


function parseError(e: unknown) : string {
  if (e instanceof Error) {
    return e.message;
  }
  if (typeof e === 'string') {
    return e;
  }
  return 'Unknown error';
}

const s3 = new S3({region: 'il-central-1'});

const logs:string[] = [];
async function downloadFile(url: string, fileName: string, index: number) {
  const res = await fetch(url);
  const contentType = res.headers.get('content-type');
  if (contentType == null) {
    logs.push(`Skipping ${url} because it has no content type`);
    console.error(`Skipping ${url} because it has no content type`);
    return;
  }
  if (['text/html; charset=utf-8', 'video/webm'].includes(contentType)) {
    console.log(`Skipping ${url} because ${contentType} it's not a mp4`);
    return;
  }
  if (contentType == null || !contentType.startsWith('video/')) {
    logs.push(`Skipping ${url} because ${contentType} it's not a video`);
    console.error(`Skipping ${url} because ${contentType} it's not a video`);
    return;
  }
  const suffix = contentType.split('/')[1];
  if (suffix  !== 'mp4') {
    logs.push(`Skipping ${url} because ${contentType} it's not a mp4`);
    console.error(`Skipping ${url} because ${contentType} it's not a mp4`);
    return;
  }
  const fullFileName = fileName.endsWith('.mp4') ? fileName : `${fileName}.mp4`;
  const destination = path.resolve('./downloads', fullFileName);
  const outputFileName = `${index}.webm`;
  if (!existsSync(destination)) {
    console.log(`Downloading ${fullFileName}`, {index});

    const fileStream = createWriteStream(destination, { flags: 'wx' });
    if (res.body == null) {
      throw new Error('Response body is empty');
    }
    await finished(Readable.fromWeb(res.body).pipe(fileStream));
  } else {
    const remoteRes = await fetch('https://mp4-wiki-files.s3.il-central-1.amazonaws.com/' + outputFileName);
    if (remoteRes.status === 404) {
      console.error(url + ' not uploaded', {index});
      logs.push(url + ' not uploaded');
    } else {
      console.log(url + ' already uploaded', {index});
      return;
    }
  }
  console.log(`Converting ${fullFileName}`, {index});
  await convert(`./downloads/${fullFileName}`, `./downloads/${outputFileName}`);
  console.log(`Converted ${fullFileName}`, {index});
  const file = await fs.readFile('./downloads/' + outputFileName);
  console.log(`Uploading ${outputFileName}`);
  await s3.putObject({
    Bucket: 'mp4-wiki-files',
    Key: outputFileName,
    Body: file,
    ACL: 'public-read',
  });
  console.log(`Uploaded ${outputFileName}`);

}

async function main() {
  if (!existsSync('downloads')) {
    await fs.mkdir('downloads');
  }
  const text = await fs.readFile('links.txt', 'utf-8');

  const links = text.split('\n');


  for (let i = 0; i < links.length; i++) {
    const link = links[i];
    if (link === '') {
      continue;
    }
    try {
      await downloadFile(link, path.basename(link), i);
    } catch (e) {
      console.error(`Failed to download ${link}: ${parseError(e)}`);
    }
  }

  await fs.writeFile('logs.txt', logs.join('\n'), 'utf-8');
}

main();
