import React from 'react';

import Link from '@docusaurus/Link';
import styles from './styles.module.css';

function ComponentCard(props) {
  const {
    type,
    component,
  } = props;

  return (
    <Link to={`/docs/components/${type}/${component.name}`} className={styles.componentCard} aria-label={`View ${component.name} component documentation`}>
      <strong>{component.name}</strong>
    </Link>
  );
}

export default ComponentCard;
