module.exports = (() => {
  if (typeof window !== "object") {
    return {};
  }

  const host = window.location.hostname;
  let path = window.location.pathname;

  return {
    onRouteUpdate({location}) {
      if ( path === location.pathname ) {
        return;
      }
      path = location.pathname;
    },
  };
})();
