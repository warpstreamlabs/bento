/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

import fs from 'fs-extra';
import path from 'path';
import logger from '@docusaurus/logger';
import {
  parseMarkdownFile,
  normalizeUrl,
  aliasedSitePath,
  getFolderContainingFile,
  posixPath,
  Globby,
  getContentPathList,
  type SourceToPermalink,
} from '@docusaurus/utils';
import type {LoadContext, ParseFrontMatter} from '@docusaurus/types';
import { validateCookbookPostFrontMatter } from './frontMatter';
import type {
  PluginOptions,
  CookbookPost,
  CookbookContentPaths,
} from './types';

export function getSourceToPermalink(
  cookbookPosts: CookbookPost[],
): SourceToPermalink {
  return new Map(
    cookbookPosts.map(({metadata: {source, permalink}}) => [source, permalink]),
  );
}

const DATE_FILENAME_REGEX =
  /^(?<folder>.*)(?<date>\d{4}[-/]\d{1,2}[-/]\d{1,2})[-/]?(?<text>.*?)(?:\/index)?.mdx?$/;

type ParsedCookbookFileName = {
  date: Date | undefined;
  text: string;
  slug: string;
};

export function parseCookbookFileName(
  blogSourceRelative: string,
): ParsedCookbookFileName {
  const dateFilenameMatch = blogSourceRelative.match(DATE_FILENAME_REGEX);
  if (dateFilenameMatch) {
    const {folder, text, date: dateString} = dateFilenameMatch.groups!;
    // Always treat dates as UTC by adding the `Z`
    const date = new Date(`${dateString!}Z`);
    const slugDate = dateString!.replace(/-/g, '/');
    const slug = `/${slugDate}/${folder!}${text!}`;
    return {date, text: text!, slug};
  }
  const text = blogSourceRelative.replace(/(?:\/index)?\.mdx?$/, '');
  const slug = `/${text}`;
  return {date: undefined, text, slug};
}

async function parseCookbookPostMarkdownFile({
  filePath,
  parseFrontMatter,
}: {
  filePath: string;
  parseFrontMatter: ParseFrontMatter;
}) {
  const fileContent = await fs.readFile(filePath, 'utf-8');
  try {
    const result = await parseMarkdownFile({
      filePath,
      fileContent,
      parseFrontMatter,
      removeContentTitle: true,
    });
    return {
      ...result,
      frontMatter: validateCookbookPostFrontMatter(result.frontMatter),
    };
  } catch (err) {
    logger.error`Error while parsing blog post file path=${filePath}.`;
    throw err;
  }
}

async function processCookbookSourceFile(
  cookbookSourceRelative: string,
  contentPaths: CookbookContentPaths,
  context: LoadContext,
  options: PluginOptions,
): Promise<CookbookPost | undefined> {
  const {
    siteConfig: {baseUrl, markdown: {parseFrontMatter}},
    siteDir,
    i18n,
  } = context;
  const {
    routeBasePath,
  } = options;

  // Lookup in localized folder in priority
  const cookbookDirPath = await getFolderContainingFile(
    getContentPathList(contentPaths),
    cookbookSourceRelative,
  );

  const cookbookSourceAbsolute = path.join(cookbookDirPath, cookbookSourceRelative);

  const {frontMatter, content, contentTitle, excerpt} =
    await parseCookbookPostMarkdownFile({
      filePath: cookbookSourceAbsolute,
      parseFrontMatter,
    });

  const aliasedSource = aliasedSitePath(cookbookSourceAbsolute, siteDir);

  if (frontMatter.draft && process.env.NODE_ENV === 'production') {
    return undefined;
  }

  if (frontMatter.id) {
    logger.warn`name=${'id'} header option is deprecated in path=${cookbookSourceRelative} file. Please use name=${'slug'} option instead.`;
  }

  const parsedCookbookFileName = parseCookbookFileName(cookbookSourceRelative);

  const title = frontMatter.title ?? contentTitle ?? parsedCookbookFileName.text;
  const description = frontMatter.description ?? excerpt ?? '';

  const slug = frontMatter.slug ?? parsedCookbookFileName.slug;

  const permalink = normalizeUrl([baseUrl, routeBasePath, slug]);

  return {
    id: slug,
    metadata: {
      permalink,
      source: aliasedSource,
      title,
      description,
    },
    content,
  };
}

export async function generateCookbookPosts(
  contentPaths: CookbookContentPaths,
  context: LoadContext,
  options: PluginOptions,
): Promise<CookbookPost[]> {
  const {include, exclude} = options;

  if (!(await fs.pathExists(contentPaths.contentPath))) {
    return [];
  }

  const cookbookSourceFiles = await Globby(include, {
    cwd: contentPaths.contentPath,
    ignore: exclude,
  });

  const cookbookPosts = (
    await Promise.all(
      cookbookSourceFiles.map(async (cookbookSourceFile: string) => {
        try {
          return await processCookbookSourceFile(
            cookbookSourceFile,
            contentPaths,
            context,
            options,
          );
        } catch (err) {
          logger.error`Processing of cookbook source file path=${cookbookSourceFile} failed.`;
          throw err;
        }
      }),
    )
  ).filter(Boolean) as CookbookPost[];
  return cookbookPosts;
}
