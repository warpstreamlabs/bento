// Allows docs to import source files from the repository as strings, so that
// they can be rendered with <CodeBlock> and never drift from the source:
//
//   import messageProto from '../../../../resources/protos/message.proto';
module.exports = function(context, options) {
  return {
    name: 'raw-source',
    configureWebpack() {
      return {
        module: {
          rules: [
            {
              test: /\.proto$/,
              type: 'asset/source',
            },
          ],
        },
      };
    },
  };
};
