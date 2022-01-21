package dev.psmolinski.kafka.ssl;

import org.objectweb.asm.*;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;

public class PatchDefaultSslEngineFactory implements ClassFileTransformer  {

    public static void premain(String agentArgs, Instrumentation instrumentation) throws Exception {
        instrumentation.addTransformer(new PatchDefaultSslEngineFactory());
    }

    public byte[]
    transform(  ClassLoader         loader,
                String              className,
                Class<?>            classBeingRedefined,
                ProtectionDomain protectionDomain,
                byte[]              classfileBuffer)
            throws IllegalClassFormatException {

        if (!className.equals("org/apache/kafka/common/security/ssl/DefaultSslEngineFactory")) {
            return null;
        }

        ClassReader cr = new ClassReader(classfileBuffer);
        // COMPUTE_MAXS flag is needed when the injected call parameters frame
        // is larger than any frame used in the original code
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cr.accept(new TransformerVisitor(cw), 0);
        return cw.toByteArray();

    }

    private static class TransformerVisitor extends ClassVisitor {
        public TransformerVisitor(ClassVisitor cv) {
            super(Opcodes.ASM9, cv);
        }

        @Override
        public MethodVisitor
        visitMethod( int access,
                     String name,
                     String descriptor,
                     String signature,
                     String[] exceptions) {

            if ("createKeystore".equals(name)) {
                return patchCreateKeystore(access, name, descriptor, signature, exceptions);
            }

            if ("createTruststore".equals(name)) {
                return patchCreateTruststore(access, name, descriptor, signature, exceptions);
            }

            return super.visitMethod(access, name, descriptor, signature, exceptions);

        }

        private MethodVisitor
        patchCreateKeystore( int access,
                                  String name,
                                  String descriptor,
                                  String signature,
                                  String[] exceptions) {

            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);

            return new MethodVisitor(api, mv) {
                @Override
                public void visitCode() {
                    // populate the stack
                    mv.visitVarInsn(Opcodes.ALOAD, 2); // path
                    // call the method
                    mv.visitMethodInsn(
                            Opcodes.INVOKESTATIC,
                            "dev/psmolinski/kafka/ssl/PatchDefaultSslEngineFactory",
                            "nullify",
                            "(Ljava/lang/String;)Ljava/lang/String;",
                            false);
                    // replace parameter path
                    mv.visitVarInsn(Opcodes.ASTORE, 2);
                    // continue with the original bytecode
                    super.visitCode();

                }
            };

        }

        private MethodVisitor
        patchCreateTruststore( int access,
                             String name,
                             String descriptor,
                             String signature,
                             String[] exceptions) {

            MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);

            return new MethodVisitor(api, mv) {
                @Override
                public void visitCode() {
                    // populate the stack
                    mv.visitVarInsn(Opcodes.ALOAD, 1); // path
                    // call the method
                    mv.visitMethodInsn(
                            Opcodes.INVOKESTATIC,
                            "dev/psmolinski/kafka/ssl/PatchDefaultSslEngineFactory",
                            "nullify",
                            "(Ljava/lang/String;)Ljava/lang/String;",
                            false);
                    // replace parameter path
                    mv.visitVarInsn(Opcodes.ASTORE, 1);
                    // continue with the original bytecode
                    super.visitCode();

                }
            };

        }

    }

    public static String nullify(String text) {
        if (text!=null && "".equals(text.trim())) {
            return null;
        } else {
            return text;
        }
    }

}
