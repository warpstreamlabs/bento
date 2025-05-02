import React from 'react';

import Link from '@docusaurus/Link';

import classnames from 'classnames';

import styles from './styles.module.css';

function CookbookItem(props) {
  const {
    frontMatter,
    metadata,
  } = props;
  const {description, permalink} = metadata;
  const {title, tags = [], featured} = frontMatter;

  return (
    <div className={styles.cookbookItemWrapper}>
      <Link to={permalink} className={classnames(styles.cookbookPostItem)}>
        <article>
          {featured && (
            <div className={styles.featuredTag}>Featured</div>
          )}
          <h2>{title}</h2>
          <div className={styles.description}>{description}</div>
          
          {tags && tags.length > 0 && (
            <div className={styles.tagContainer}>
              {tags.map((tag) => (
                <span key={tag} className={styles.tag}>
                  {tag}
                </span>
              ))}
            </div>
          )}
        </article>
      </Link>
    </div>
  );
}

export default CookbookItem;
