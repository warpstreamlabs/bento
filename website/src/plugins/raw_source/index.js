// Allows docs to import source files from the repository as strings, so that
// they can be rendered with <CodeBlock> and never drift from the source:
//
//   import messageProto from '../../../../resources/protos/message.proto';
//   import abi from '../../../../resources/wasm/abi.wat';
module.exports = function(context, options) {
  return {
    name: 'raw-source',
    configureWebpack() {
      return {
        module: {
          rules: [
            {
              test: /\.(proto|wat)$/,
              type: 'asset/source',
            },
          ],
        },
      };
    },
  };
};
