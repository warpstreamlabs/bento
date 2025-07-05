import React, { useEffect, useRef, useCallback } from "react";
import { useColorMode } from "@docusaurus/theme-common";

export default function PlaygroundIframe({ src, ...props }) {
  const iframeRef = useRef(null);
  const { colorMode } = useColorMode();

  // Send theme to iframe
  const sendThemeToIframe = useCallback(() => {
    const iframe = iframeRef.current;
    if (iframe?.contentWindow) {
      iframe.contentWindow.postMessage(
        { type: "docusaurus-theme-change", theme: colorMode },
        "*"
      );
    }
  }, [colorMode]);

  // Message handler
  const handleMessage = useCallback(
    (event) => {
      if (event.data?.type === "request-theme") {
        sendThemeToIframe();
      }
    },
    [sendThemeToIframe]
  );

  // Load handler
  const handleLoad = useCallback(() => {
    // If needed, keep the timeout for iframe readiness
    setTimeout(sendThemeToIframe, 100);
  }, [sendThemeToIframe]);

  useEffect(() => {
    const iframe = iframeRef.current;
    if (!iframe) return;

    sendThemeToIframe();
    iframe.addEventListener("load", handleLoad);
    window.addEventListener("message", handleMessage);

    return () => {
      iframe.removeEventListener("load", handleLoad);
      window.removeEventListener("message", handleMessage);
    };
  }, [handleLoad, handleMessage, sendThemeToIframe]);

  return <iframe ref={iframeRef} src={src} {...props} />;
}
